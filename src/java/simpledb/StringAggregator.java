package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private Map<Field, Integer> groupMap;
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField afield = (StringField) tup.getField(this.afield);
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        String newValue = afield.getValue();
        if(gbfield != null && gbfield.getType() != gbfieldtype)
            throw new IllegalArgumentException("given tuple has wrong type");
        if(!this.groupMap.containsKey(gbfield))
            this.groupMap.put(gbfield, 1);
        else
            this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggregateIterator(this.groupMap, this.gbfieldtype);
    }
}

class AggregateIterator implements OpIterator {
    protected Iterator<Map.Entry<Field, Integer>> it;
    private Map<Field, Integer> groupMap;
    private Type itgbfieldtype;
    public TupleDesc td;

    public AggregateIterator(Map<Field, Integer> groupMap, Type gbfieldtype) {
        this.groupMap = groupMap;
        this.itgbfieldtype = gbfieldtype;
        if(this.itgbfieldtype == null)
            this.td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateVal"});
        else
            this.td = new TupleDesc(new Type[] {this.itgbfieldtype, Type.INT_TYPE}, new String[] {"groupVal", "aggregateVal"});
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.it = groupMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        Map.Entry<Field, Integer> entry = it.next();
        Field f = entry.getKey();
        Tuple t = new Tuple(td);
        if(f == null)
            t.setField(0, new IntField(entry.getValue()));
        else {
            t.setField(0, f);
            t.setField(1, new IntField(entry.getValue()));
        }
        return t;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.it = groupMap.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void close() {
        this.it = null;
        this.td = null;
    }
}
