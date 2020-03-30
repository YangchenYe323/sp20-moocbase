package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        private boolean hasNetRightPage;
        // The current record on the left page
        private Record leftRecord = null;
        private Record rightRecord = null;
        // The next record to return
        private Record nextRecord = null;

        //max block size, numbuffer -2
        int maxPage;

        //leftBlock is an array of pages holding
        //all the in-memory left page
        List<Page> leftBlock;
        Page leftPage;
        int currentPageIndex;
        //buffer to stream right pages
        Page rightPage;

        private BNLJIterator() {
            super();

            this.maxPage = BNLJOperator.this.numBuffers - 2;

            leftBlock = new ArrayList<>();
            rightPage = null;

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();
            leftRecord = leftRecordIterator.next();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();
            rightRecord = rightRecordIterator.next();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {

            //bring next maxPage pages to memory, initialize an record iterator over that block
            leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, maxPage);
            leftRecordIterator.markNext();

            //there are no more pages, clean up
            if (!leftRecordIterator.hasNext()){
                leftRecordIterator = null;
                leftIterator = null;
            }

        }

        /*private void fetchNextLeftRecordInBlock(){
            if (leftRecordIterator == null) throw new NoSuchElementException("No more records");

            //iterate through the block in memory
            if (leftRecordIterator.hasNext())
                leftRecord = leftRecordIterator.next();
            else{
                //bring another block
                fetchNextLeftBlock();
            }

        }*/



        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the left relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            //bring one more page into memory, initialize an iterator over it
            rightRecordIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), rightIterator, 1);

            //this iteration is over, set it to null
            if (!rightRecordIterator.hasNext()){
                rightRecordIterator = null;
            }

        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {


            nextRecord = null;
            while (nextRecord == null){
                if (leftRecord != null) {
                    DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue))
                        nextRecord = joinRecords(leftRecord, rightRecord);

                    leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
                } else{
                    //in this case we reset the left record iterator
                    //and compare the block of left record with the next
                    //right record in the page
                    if (rightRecordIterator.hasNext()){
                        leftRecordIterator.reset();
                        leftRecord = leftRecordIterator.next();
                        rightRecord = rightRecordIterator.next();
                    } else{ //now we bring in another right page
                        fetchNextRightPage();
                        //not the last page
                        if (rightRecordIterator != null){
                            rightRecord = rightRecordIterator.next();
                            leftRecordIterator.reset();
                            leftRecord = leftRecordIterator.next();
                        }
                        else{
                            fetchNextLeftBlock();
                            //now we are done!
                            if (leftRecordIterator == null) throw new NoSuchElementException();
                            leftRecord = leftRecordIterator.next();
                            rightIterator.reset();
                            fetchNextRightPage();
                            rightRecord = rightRecordIterator.next();
                        }
                    }
                }
            }

        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
