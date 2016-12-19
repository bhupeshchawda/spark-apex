package com.datatorrent.example.scala;
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{RDD, _}

import scala.math.Ordering
import scala.reflect.ClassTag

abstract class ApexRDDs[T: ClassTag](prev: RDD[T]) extends RDD[T](prev) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  override protected def getPartitions: Array[Partition] = ???

  def scalaInt(integer: java.lang.Integer): Int={
    try{
      integer.toInt
    }catch {
      case e: Exception => 0
    }

  }

}

object ApexRDDs
{
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

