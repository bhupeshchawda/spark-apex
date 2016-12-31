package com.datatorrent.example.utils;

import com.datatorrent.example.ApexRDD;
import junit.framework.Assert;
import org.apache.spark.Partitioner;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassTag;

/**
 * Created by anurag on 12/12/16.
 */
public class PairApexRDDFunctions<K,V> extends PairRDDFunctions<K,V> {
    public ApexRDD<Tuple2<K,V>> apexRDD;
    Logger  log = LoggerFactory.getLogger(PairApexRDDFunctions.class);
    public PairApexRDDFunctions(ApexRDD<Tuple2<K, V>> self, ClassTag<K> kt, ClassTag<V> vt, Ordering<K> ord) {
        super(self, kt, vt, ord);
        this.apexRDD=self;
    }
    @Override
    public <C> RDD<Tuple2<K, C>> combineByKey(Function1<V, C> createCombiner, Function2<C, V, C> mergeValue,
                                              Function2<C, C, C> mergeCombiners, Partitioner partitioner,
                                              boolean mapSideCombine, Serializer serializer) {
        ApexRDD<Tuple2<K,C>> temp =(ApexRDD<Tuple2<K,C>> )super.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer);
        temp.dag=apexRDD.dag;
        log.debug("combineByKey is called");
        Assert.assertTrue(false);
        return temp;
    }

    @Override
    public <C> RDD<Tuple2<K, C>> combineByKey(Function1<V, C> createCombiner,
                                              Function2<C, V, C> mergeValue, Function2<C, C, C> mergeCombiners,
                                              int numPartitions) {
        ApexRDD<Tuple2<K,C>>  temp=( ApexRDD<Tuple2<K,C>> ) super.combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions);
        temp.dag=apexRDD.dag;
        return temp;
    }
}
