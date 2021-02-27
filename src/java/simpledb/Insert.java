package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final int tableid;
    private final TransactionId tid;
    private final TupleDesc tupleDesc;
    private int count;
    private boolean called;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableid = tableId;
        this.count = -1;
        this.called = false;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"number of inserted tuples"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        this.count = 0;
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.count = -1;
        this.called = false;
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.count = 0;
        this.called = false;
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.called)
                return null;
        called = true;
        Tuple t = new Tuple(tupleDesc);
        while(child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableid, tuple);
                this.count++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        t.setField(0, new IntField(this.count));
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
