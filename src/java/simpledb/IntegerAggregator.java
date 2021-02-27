package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbfield;
    private final Type gbfieldType;
    private int afield;
    private Op what;

    private Map<Field, Integer> groupMap;
    private Map<Field, List<Integer>> avgMap;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
        this.avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField afield = (IntField) tup.getField(this.afield);
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        int newValue = afield.getValue();
        if(gbfield != null && gbfield.getType() != this.gbfieldType)
            throw new IllegalArgumentException("given tuple's gbfieldType is wrong");
        else {
            switch (this.what){
                case MIN:
                    if(!this.groupMap.containsKey(gbfield))
                        this.groupMap.put(gbfield, newValue);
                    else
                        this.groupMap.put(gbfield, Math.min(newValue, this.groupMap.get(gbfield)));
                    break;
                case MAX:
                    if(!this.groupMap.containsKey(gbfield))
                        this.groupMap.put(gbfield, newValue);
                    else
                        this.groupMap.put(gbfield, Math.max(newValue, this.groupMap.get(gbfield)));
                    break;
                case COUNT:
                    if(!this.groupMap.containsKey(gbfield))
                        this.groupMap.put(gbfield, 1);
                    else
                        this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
                    break;
                case SUM:
                    if(!this.groupMap.containsKey(gbfield))
                        this.groupMap.put(gbfield, newValue);
                    else
                        this.groupMap.put(gbfield, this.groupMap.get(gbfield) + newValue);
                    break;
                case AVG:
                    if(!this.avgMap.containsKey(gbfield)) {
                        List<Integer> list = new ArrayList<>();
                        list.add(newValue);
                        this.avgMap.put(gbfield, list);
                    } else
                        this.avgMap.get(gbfield).add(newValue);
                    break;
                case SC_AVG:    break;
                case SUM_COUNT: break;
                default:
                    throw new IllegalArgumentException("aggregate function not supported");
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntAggIterator(groupMap, avgMap, gbfieldType, what);
    }
}
class IntAggIterator extends AggregateIterator {

    private Iterator<Map.Entry<Field, List<Integer>>> avgIter;
    private Map<Field, List<Integer>> avgMap;
    private Aggregator.Op what;

    public IntAggIterator(Map<Field, Integer> groupMap, Map<Field, List<Integer>> avgMap, Type gbfieldtype, Aggregator.Op what) {
        super(groupMap, gbfieldtype);
        this.avgMap = avgMap;
        this.what = what;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        if(what.equals(Aggregator.Op.AVG))
            avgIter = avgMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(what.equals(Aggregator.Op.AVG))
            return avgIter.hasNext();
        else
            return super.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(what.equals(Aggregator.Op.AVG)) {
            Map.Entry<Field, List<Integer>> entry = avgIter.next();
            Field f = entry.getKey();
            List<Integer> list = entry.getValue();
            int res = 0;
            for(int i : list)
                res += i;
            res /= list.size();
            Tuple tuple = new Tuple(td);
            if(f == null)
                tuple.setField(0, new IntField(res));
            else {
                tuple.setField(0, f);
                tuple.setField(1, new IntField(res));
            }
            return tuple;
        } else
            return super.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        super.rewind();
    }

    @Override
    public void close() {
        super.close();
        what = null;
    }
}
