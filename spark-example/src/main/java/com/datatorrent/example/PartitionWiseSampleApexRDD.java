package com.datatorrent.example;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.random.BernoulliCellSampler;
import org.apache.spark.util.random.RandomSampler;
import scala.reflect.ClassTag;

/**
 * Created by harsh on 9/12/16.
 */
/*public class PartitionWiseSampleApexRDD<T,U> extends Partition implements Serializable{

    public int index() {
        return 0;
    }

    public int hashCode() {
        return super.hashCode();
    }
}*/
public class PartitionWiseSampleApexRDD<T,U> extends ApexRDD<T> {
    public RandomSampler<T,T> sampler;
    public RDD<T> prev;
    public  long seed;
    double [] weight;


    public PartitionWiseSampleApexRDD(RDD<T> rdd, ClassTag<T> classTag, double[] weight){
        super(rdd,classTag);
        prev = rdd;
        double lb= weight[0];
        double ub= weight[1];
        sampler = new BernoulliCellSampler<T>(lb,ub,true);
        seed = RandomUtils.nextLong();
    }
    public PartitionWiseSampleApexRDD(ApexContext ac){
        super(ac);

    }

}


