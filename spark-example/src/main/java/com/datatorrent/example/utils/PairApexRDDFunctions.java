package com.datatorrent.example.utils;

import org.apache.spark.Partitioner;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.Serializer;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassTag;

/**
 * Created by anurag on 12/12/16.
 */
public class PairApexRDDFunctions<K,V> {//extends PairRDDFunctions<K,V> {

    public PairApexRDDFunctions() {
//        super();
    }

//    @Override
    public <C> RDD<Tuple2<K, C>> combineByKey(Function1<V, C> createCombiner, Function2<C, V, C> mergeValue, Function2<C, C, C> mergeCombiners, Partitioner partitioner, boolean mapSideCombine, Serializer serializer) {
//        return super.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer);
        return null;
    }

//    @Override
    public <C> RDD<Tuple2<K, C>> combineByKey(Function1<V, C> createCombiner, Function2<C, V, C> mergeValue, Function2<C, C, C> mergeCombiners, int numPartitions) {
//        return super.combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions);
        return null;
    }
}
