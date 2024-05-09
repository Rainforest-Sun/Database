package simpledb;

import java.util.*;
import java.io.*;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId tid;
    private DbIterator child;
    private int cnt;
    private TupleDesc td;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        Type[] types = new Type[1];
        types[0] = Type.INT_TYPE;
        this.td = new TupleDesc(types);
        this.cnt = -1;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        this.cnt = 0;
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            tuples.add(child.next());
        }
        for (Tuple tuple : tuples) {
            try{
                Database.getBufferPool().deleteTuple(tid, tuple);
                this.cnt++;
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    public void close() {
        // some code goes here
        this.cnt = -1;
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.cnt == -1) return null;
        Tuple delTuple = new Tuple(this.td);
        delTuple.setField(0, new IntField(this.cnt));
        this.cnt = -1;
        return delTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[1];
        children[0] = this.child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
