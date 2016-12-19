package com.datatorrent.example.apexscala

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.ClassTag

/**
  * Created by anurag on 16/12/16.
  */

 class ScalaApexRDD[T:ClassTag](
                                @transient private var sc: SparkContext,
                                @transient private var deps: Seq[Dependency[_]]
                              ) extends RDD[T](sc,Nil) {

  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  override protected def getPartitions: Array[Partition] = ???
}
object ScalaApexRDD extends {
  implicit  def rddToPairRDDFunctions[K,V](rdd:RDD[(K,V)])
                                         ( implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V]={
    println("We are here")
    new PairRDDFunctions[K,V](rdd)
  }
}