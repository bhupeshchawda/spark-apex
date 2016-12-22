package com.datatorrent.example.apexscala

import com.datatorrent.example.utils.MyDAG
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by anurag on 16/12/16.
  */

 class ScalaApexRDD[T:ClassTag](
                                @transient private var sc: SparkContext,
                                @transient private var deps: Seq[Dependency[_]]
                              ) extends RDD[T](sc,Nil) with Serializable{

  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))
  val dag=new MyDAG()
  override def treeAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2): U = {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")

    val cleanSeqOp = seqOp
    val cleanCombOp = combOp
    val aggregatePartition =
      (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    var partiallyAggregated = mapPartitions(it => Iterator(aggregatePartition(it)))
    var numPartitions = partiallyAggregated.partitions.length
    val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
    // If creating an extra level doesn't help reduce
    // the wall-clock time, we stop tree aggregation.

//    // Don't trigger TreeAggregation when it doesn't save wall-clock time
//    while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
//      numPartitions /= scale
//      val curNumPartitions = numPartitions
//      partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
//        (i, iter) => iter.map((i % curNumPartitions, _))
//      }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values
//    }
    partiallyAggregated.reduce(cleanCombOp)
  }
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  override protected def getPartitions: Array[Partition] = ???
}
object ScalaApexRDD extends {
  implicit  def rddToPairRDDFunctions[K,V](rdd:ScalaApexRDD[(K,V)])
                                         ( implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairApexRDDFunction[K, V]={
    println("We are here")
    new PairApexRDDFunction[K,V](rdd)
  }
}