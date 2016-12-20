package com.datatorrent.example.utils;

import com.datatorrent.example.ApexRDD;
import org.apache.spark.Partitioner;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.Serializer;
import org.slf4j.Logger;import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Map;
import scala.reflect.ClassTag;

/**
 * Created by anurag on 12/12/16.
 */
public class PairApexRDDFunctions<K,V> extends PairRDDFunctions<K,V>{
    public ApexRDD<Tuple2<K,V>> apexRDD;
    org.slf4j.Logger log = LoggerFactory.getLogger(PairApexRDDFunctions.class);
    public PairApexRDDFunctions(ApexRDD<Tuple2<K,V>> self, ClassTag<K> kt, ClassTag<V> vt , ClassTag<V> ord ) {
        super(self,kt,vt,null);
        this.apexRDD=self;
    }

    @Override
    public Map<K, Object> countByKey() {
        return super.countByKey();
    }

    @Override
    public RDD<Tuple2<K, V>> reduceByKey(Function2<V, V, V> func, int numPartitions) {
        return super.reduceByKey(func, numPartitions);
    }

    @Override
    public RDD<Tuple2<K, V>> reduceByKey(Partitioner partitioner, Function2<V, V, V> func) {
        return super.reduceByKey(partitioner, func);
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
