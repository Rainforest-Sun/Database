package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;

    private HashMap<Field, Integer> groupByMap;
    private HashMap<Field, Integer> countMap;
    private TupleDesc aTD;

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.groupByMap = new HashMap<>();
        this.countMap = new HashMap<>();
        if (gbfield == NO_GROUPING) this.aTD = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        else this.aTD = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
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
        Field groupField = null;
        if (this.gbField != NO_GROUPING) groupField = tup.getField(this.gbField);
        Integer preVal = groupByMap.get(groupField);
        int curVal = ((IntField) tup.getField(this.aField)).getValue();
        Integer newVal = null;
        switch (this.op) {
            case MIN: {
                if (preVal == null) newVal = curVal;
                else newVal = Integer.min(preVal, curVal);
                break;
            } 
            case MAX: {
                if (preVal == null) newVal = curVal;
                else newVal = Integer.max(preVal, curVal);
                break;
            }
            case AVG: {
                if (preVal == null) newVal = curVal;
                else newVal = preVal + curVal;
                Integer cnt = countMap.getOrDefault(groupField, 0);
                countMap.put(groupField, cnt + 1);
                break;
            }
            case SUM: {
                if (preVal == null) newVal = curVal;
                else newVal = preVal + curVal;
                break;
            }
            case COUNT: {
                if (preVal == null) newVal = 1;
                else newVal = preVal + 1;
                break;
            }
        }
        groupByMap.put(groupField, newVal);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> aTP = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : groupByMap.entrySet()) {
            Tuple tp = new Tuple(this.aTD);
            Integer val = entry.getValue();
            if (this.op == Op.AVG)
                val = val / countMap.get(entry.getKey());
            if (this.gbField == NO_GROUPING) {
                tp.setField(0, new IntField(val));
            } 
            else {
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(val));
            }
            aTP.add(tp);
        }
        return new TupleIterator(this.aTD, aTP);
    }

}
