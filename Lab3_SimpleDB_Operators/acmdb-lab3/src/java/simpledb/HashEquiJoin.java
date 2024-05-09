package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private JoinPredicate jp;
    private DbIterator child1;
    private DbIterator child2;
    private TupleDesc joinedtd;
    private ArrayList<Tuple> joinedtp;
    private HashMap<Field, ArrayList<Tuple>> map;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.jp = p;
        this.child1 = child1;
        this.child2 = child2;
        this.joinedtd = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        this.joinedtp = new ArrayList<>();
        this.map = new HashMap<>();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.jp;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.joinedtd;
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(this.jp.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(this.jp.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
        int len1 = child1.getTupleDesc().numFields();
        int len2 = child2.getTupleDesc().numFields();
        while (child2.hasNext()) {
            Tuple tp = child2.next();
            Field f = tp.getField(jp.getField2());
            if (!map.containsKey(f)) map.put(f, new ArrayList<>());
            map.get(f).add(tp);
        }
        while (child1.hasNext()) {
            Tuple tp1 = child1.next();
            Field f1 = tp1.getField(jp.getField1());
            for (Field f : map.keySet()) {
                if (f1.compare(jp.getOperator(), f)) {
                    for (Tuple tp2 : map.get(f)) {
                        Tuple joined = new Tuple(this.joinedtd);
                        for (int i = 0; i < len1; ++i) 
                            joined.setField(i, tp1.getField(i));
                        for (int i = 0; i < len2; ++i) 
                            joined.setField(len1 + i, tp2.getField(i));
                            this.joinedtp.add(joined);
                    }
                }
            }
        }
        this.listIt = joinedtp.iterator();
    }

    public void close() {
        // some code goes here
        this.map.clear();
        this.joinedtp.clear();
        this.listIt = null;
        this.child2.close();
        this.child1.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.listIt.hasNext()) return listIt.next();
        else return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[2];
        children[0] = this.child1;
        children[1] = this.child2;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }
    
}