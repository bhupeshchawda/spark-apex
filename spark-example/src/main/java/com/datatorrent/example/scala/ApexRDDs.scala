package com.datatorrent.example.scala;
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{RDD, _}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.math.Ordering
import scala.reflect.ClassTag

//abstract class ApexRDDs[T: ClassTag](prev: RDD[T]) extends RDD[T](prev) {
abstract class ApexRDDs[T: ClassTag](
                                     @transient private var sc: SparkContext,
                                     @transient private var deps: Seq[Dependency[_]]
                                   ) extends RDD[T](sc,Nil){

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  override protected def getPartitions: Array[Partition] = ???

  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

  override def sparkContext: SparkContext = sc

  def scalaInt(integer: java.lang.Integer): Int={
    try{
      integer.toInt
    }catch {
      case e: Exception => 0
    }
  }

  def scalaMap[K,V](map: java.util.HashMap[K,V]): mutable.Map[K, V]={
    val scalamap = map.asScala;
    return scalamap
  }

  def getFunc[U](f: (Iterator[T]) => Iterator[U]): (TaskContext, Int, Iterator[T]) => Iterator[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(iter)
    func
  }
/*
  def scalaIter(iterator: java.util.Iterator): Iterator={
    val iter = iterator.asInstanceOf[Iterator]
    iter
  }
*/


}

object ApexRDDs {
  implicit class ApexRDDs[T: ClassTag](rdd: RDD[T])
  {
    implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
                                            (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
      new PairRDDFunctions(rdd)
    }
  }
  implicit def javaToScalaInt(d: java.lang.Integer) = d.intValue
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
                                          (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

}


