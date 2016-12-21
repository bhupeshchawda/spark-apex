package com.datatorrent.example.scala

import org.apache.spark.annotation.Experimental
import org.apache.spark.{InterruptibleIterator, _}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

/**
  * Created by harsh on 21/12/16.
  */
class PairApexRDDFunctions[K, V](self: RDD[(K, V)])
                            (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
 extends Serializable{

  def combineByKeyWithClassTag[C](
                                   createCombiner: V => C,
                                   mergeValue: (C, V) => C,
                                   mergeCombiners: (C, C) => C,
                                   partitioner: Partitioner,
                                   mapSideCombine: Boolean = true,
                                   serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("Default partitioner cannot partition array keys.")
      }
    }
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
    }
  }

  /**
    * Generic function to combine the elements for each key using a custom set of aggregation
    * functions. This method is here for backward compatibility. It does not provide combiner
    * classtag information to the shuffle.
    *
    * @see [[combineByKeyWithClassTag]]
    */
  def combineByKey[C](
                       createCombiner: V => C,
                       mergeValue: (C, V) => C,
                       mergeCombiners: (C, C) => C,
                       partitioner: Partitioner,
                       mapSideCombine: Boolean = true,
                       serializer: Serializer = null): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,
      partitioner, mapSideCombine, serializer)(null)
  }

  /**
    * Simplified version of combineByKeyWithClassTag that hash-partitions the output RDD.
    * This method is here for backward compatibility. It does not provide combiner
    * classtag information to the shuffle.
    *
    * @see [[combineByKeyWithClassTag]]
    */
  def combineByKey[C](
                       createCombiner: V => C,
                       mergeValue: (C, V) => C,
                       mergeCombiners: (C, C) => C,
                       numPartitions: Int): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, numPartitions)(null)
  }

  /**
    * :: Experimental ::
    * Simplified version of combineByKeyWithClassTag that hash-partitions the output RDD.
    */
  @Experimental
  def combineByKeyWithClassTag[C](
                                   createCombiner: V => C,
                                   mergeValue: (C, V) => C,
                                   mergeCombiners: (C, C) => C,
                                   numPartitions: Int)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,
      new HashPartitioner(numPartitions))
  }

}
