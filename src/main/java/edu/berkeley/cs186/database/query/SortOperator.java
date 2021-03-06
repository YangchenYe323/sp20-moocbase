package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.Page;

import java.util.PriorityQueue;

import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {

        Iterator<Record> iter = run.iterator();

        List<Record> unsorted = new ArrayList<>();
        while (iter.hasNext()){
            unsorted.add(iter.next());
        }
        unsorted.sort(this.comparator);
        Record[] sorted = new Record[unsorted.size()];
        for (int i = 0; i < unsorted.size(); ++i){
            sorted[i] = unsorted.get(i);
        }

        return createRunFromIterator(new ArrayBacktrackingIterator<>(sorted));
    }

    /**
     * Given a list of sorted runs, returns a NEW run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {

        Run sortedrun = createRun();

        PriorityQueue<Pair<Record, Integer>> workQueue = new PriorityQueue<>((o1, o2) -> comparator.compare(o1.getFirst(), o2.getFirst()));
        List<Iterator<Record>> iterList = new ArrayList<>();
        for (Run run: runs){
            iterList.add(run.iterator());
        }

        //populate the first round
        populate(workQueue, iterList);

        while (!workQueue.isEmpty()){
            Pair<Record, Integer> nextPair = workQueue.poll();
            sortedrun.addRecord(nextPair.getFirst().getValues());
            int index = nextPair.getSecond();
            if (iterList.get(index).hasNext())
                workQueue.add(new Pair<>(iterList.get(index).next(), index));
        }

        return sortedrun;
    }

    private void populate(PriorityQueue<Pair<Record, Integer>> workQueue, List<Iterator<Record>> iterList){
        for (int i = 0; i < iterList.size(); ++i){
            if (iterList.get(i).hasNext())
                workQueue.add(new Pair<>(iterList.get(i).next(), i));
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time. It is okay for the last sorted run
     * to use less than (numBuffers - 1) input runs if N is not a
     * perfect multiple.
     */
    public List<Run> mergePass(List<Run> runs) {
        List<Run> result = new ArrayList<>();

        int groupSize = numBuffers - 1;

        for (int i = 0; i < runs.size(); i += groupSize){
            List<Run> subruns = runs.subList(i, Math.min(i + groupSize, runs.size()));
            result.add(mergeSortedRuns(subruns));
        }

        return result;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {

        //this is the number of record in one run
        int numOfRecord = transaction.getTable(tableName).getNumRecordsPerPage() * (numBuffers - 1);

        //we get record through this iterator
        Iterator<Record> iter = transaction.getTable(tableName).iterator();

        //place to hold list of our sorted runs
        List<Run> initialRun = new ArrayList<>();

        //place to hold temporary record (that is to be stored in runs)
        Record[] tmp = new Record[numOfRecord];
        //List<Record> tmpRecordList = new ArrayList<>();
        int count = 0;
        while (iter.hasNext()){
            Record nextR = iter.next();
            tmp[count] = nextR;
            //tmpRecordList.add(nextR);
            count++;
            if (count == numOfRecord){
                count = 0;
                //pass 0
                //Run run = createRun();
                //run.addRecords(tmpRecordList);
                //initialRun.add(sortRun(run));
                //tmpRecordList = new ArrayList<>();

                initialRun.add(sortRun(createRunFromIterator(new ArrayBacktrackingIterator<>(tmp))));
                tmp = new Record[numOfRecord];
            }

            //this is the last run we have, may not have numOfRecord size
            if (!iter.hasNext()){

                //Run run = createRun();
                //run.addRecords(tmpRecordList);
                //initialRun.add(sortRun(run));
                //tmpRecordList = new ArrayList<>();

                Record[] shortTmp = new Record[count];
                System.arraycopy(tmp, 0, shortTmp, 0, count);
                //pass 0
                initialRun.add(sortRun(createRunFromIterator(new ArrayBacktrackingIterator<>(shortTmp))));
            }
        }

        initialRun = mergePass(initialRun);

        //merge until we get only one run left
        while (initialRun.size() != 1){
            initialRun = mergePass(initialRun);
        }

        return initialRun.get(0).tableName();
    }

    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

}

