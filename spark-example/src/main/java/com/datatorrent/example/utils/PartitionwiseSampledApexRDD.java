package com.datatorrent.example.utils;

import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.util.random.RandomSampler;
import scala.Serializable;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * Created by anurag on 9/12/16.
 */

class PartitionwiseSampledApexRDDPartition implements Serializable, Partition {
    private final Partition prev;
    private final long seed;

    public PartitionwiseSampledApexRDDPartition(Partition prev, long seed) {
        this.prev=prev;
        this.seed=seed;
    }

    public int index() {
        return prev.index();
    }

    public int hashCode() {
        return super.hashCode();
    }
}
public class PartitionwiseSampledApexRDD<T ,U> extends ApexRDD<T> {
    public RandomSampler<T,U> randomSampler;
    public long seed;
    public ApexRDD<T> apexRDD;
    public boolean preservePartitioning;
    public ClassTag<T> classTag;
    public PartitionwiseSampledApexRDD(ApexRDD<T> apexRDD, RandomSampler<T,U> randomSampler,
                                       boolean preservervePartitioning,long seed,ClassTag<T> classTag) {
        super(apexRDD, classTag);
        this.seed=seed;
        this.randomSampler=randomSampler;
        this.apexRDD=apexRDD;
        this.preservePartitioning=preservervePartitioning;
        this.classTag=classTag;

    }

    public PartitionwiseSampledApexRDD(ApexContext ac) {
        super(ac);
    }

    @Override
    public Partition[] getPartitions() {
        ApexRDD<T> apexRDD= (ApexRDD<T>) firstParent(classTag);
        Collection<Partition> partition = Arrays.asList(apexRDD.partitions());
        Random r = new Random(seed);
        Partition[] p = (Partition[]) partition.stream().map(e -> new PartitionwiseSampledApexRDDPartition(e, r.nextLong())).toArray();
        return p;
    }

    @Override
    public Iterator<T> compute(Partition arg0, TaskContext arg1) {
        return super.compute(arg0, arg1);
    }

}
