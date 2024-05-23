package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int min;
    private int max;
    private int width;
    private int ntups;
    private int nbuckets;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        if (max - min + 1 < buckets) {
            buckets = max - min + 1;
        }
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (max - min + 1) / buckets;
        this.ntups = 0;
        this.nbuckets = buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = Math.min((v - this.min) / this.width, this.nbuckets - 1);
        this.buckets[index]++;
        this.ntups++;
    }

    private int getBucketWidth(int index) {
        if (index < this.nbuckets - 1) return this.width;
        else return (this.max - this.min + 1) - this.width * (this.nbuckets - 1);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int index = Math.min((v - this.min) / this.width, this.nbuckets - 1);
        // double selectivity = 0.0;
        switch (op) {
            case EQUALS: {
                if (v < this.min || v > this.max) return 0.0;
                else return (double) this.buckets[index] / this.getBucketWidth(index) / this.ntups;
            }
            case NOT_EQUALS: {
                return 1.0 - this.estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            case GREATER_THAN: {
                double selectivity = 0.0;
                if (v < this.min) return 1.0;
                if (v >= this.max) return 0.0;
                for (int i = index + 1; i < this.nbuckets; ++i) {
                    selectivity += (double) this.buckets[i];
                }
                int right = index * this.width + getBucketWidth(index);
                selectivity += (double) ((double) this.buckets[index] * (right - v)) / this.getBucketWidth(index);
                return selectivity / this.ntups;
            }
            case LESS_THAN: {
                double selectivity = 0.0;
                if (v <= this.min) return 0.0;
                if (v > this.max) return 1.0;
                for (int i = 0; i < index; ++i) {
                    selectivity += (double) this.buckets[i];
                }
                int left = index * this.width + 1;
                selectivity += (double) ((double) this.buckets[index] * (v - left)) / this.getBucketWidth(index);
                return selectivity / this.ntups;
            }
            case GREATER_THAN_OR_EQ: {
                return this.estimateSelectivity(Predicate.Op.GREATER_THAN, v) + this.estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            case LESS_THAN_OR_EQ: {
                return this.estimateSelectivity(Predicate.Op.LESS_THAN, v) + this.estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            default: {
                throw new IllegalArgumentException("Invalid operator");
            }
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
