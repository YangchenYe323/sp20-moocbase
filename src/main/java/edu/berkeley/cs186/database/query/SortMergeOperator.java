package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked = false;

        private SortMergeIterator() {
            super();

            //construct sort operators
            SortOperator leftSort = new SortOperator(SortMergeOperator.this.getTransaction(), getLeftTableName(),
                    Comparator.comparing(o -> o.getValues().get(SortMergeOperator.this.getLeftColumnIndex())));
            SortOperator rightSort = new SortOperator(SortMergeOperator.this.getTransaction(), getLeftTableName(),
                    Comparator.comparing(o -> o.getValues().get(SortMergeOperator.this.getRightColumnIndex())));

            //sort both input tables
            String sortedLeftTableName = leftSort.sort();
            String sortedRightTableName = rightSort.sort();

            //construct iterator over the sorted tables
            leftIterator = getTransaction().getRecordIterator(sortedLeftTableName);
            rightIterator = getTransaction().getRecordIterator(sortedRightTableName);
            rightIterator.markNext();

            fetchLeftRecord();
            fetchRightRecord();

            //if (leftRecord == null) System.out.println("LEFT");
            //if (rightRecord == null) System.out.println("RIGHT");

            fetchNextRecord();

        }

        private void fetchLeftRecord(){
            if (leftIterator.hasNext()) leftRecord = leftIterator.next();
            else throw new NoSuchElementException("No More Record");
        }

        private void fetchRightRecord(){
            rightRecord = rightIterator.hasNext() ? rightIterator.next(): null;
        }

        private void fetchNextRecord(){
            if (leftRecord == null) throw new NoSuchElementException("No more records");

            nextRecord = null;
            while (nextRecord == null){
                if (rightRecord != null){
                    DataBox leftValue = leftRecord.getValues().get(getLeftColumnIndex());
                    DataBox rightValue = rightRecord.getValues().get(getRightColumnIndex());

                    if (leftValue.equals(rightValue)){
                        if (!marked){
                            marked = true;
                            rightIterator.markPrev();
                        }

                        nextRecord = joinRecords(leftRecord, rightRecord);

                        fetchRightRecord();
                    }

                    if (!leftValue.equals(rightValue) && marked){
                        marked = false;
                        rightIterator.reset();
                        fetchLeftRecord();
                        fetchRightRecord();
                    }
                } else{
                    fetchLeftRecord();
                    rightIterator.reset();
                    fetchRightRecord();
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

            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {

            Record returnRecord = nextRecord;

            try {
                fetchNextRecord();
            } catch(NoSuchElementException e){
                nextRecord = null;
            }

            return returnRecord;

        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
