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

  def getFunc[U](f: (Iterator[T]) => Iterator[U]): (TaskContext, Int, Iterator[T]) => Iterator[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(iter)
    func
  }


//  def getFunc(f: (T, T) => T): (Iterator[T]) => Option[T] = {
//    val reducePartition: Iterator[T] => Option[T] = iter => {
//      if (iter.hasNext) {
//        Some(iter.reduceLeft(f))
//      } else {
//        None
//      }
//    }
//    reducePartition
//  }



//  def toArray(array: Array[Object]): Array[T] ={
//  val data= new Array[T](array.length)
//  val count=0
//  for ( a <- array.toIterator){
//    data(count)=a.asInstanceOf[T]
//  }
//  data
//
//}
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

//  override def take(num: Int): Array[T] = {
//    println(num)
//    var a = new  util.ArrayList[T]()
////    var a =new ArrayBuffer[T](num)
//    if(num==1){
//
//      a.add(this.collect()(0))
//      println("Collected element: " + a)
//      a.toArray
////      return a
//    }
//    var count=0;
////    for( o <-this.collect()){
////      if(count>=num)
////        return a.toArray
////      else{
////        a+=o
////        count=count+1
////      }
////    }
////   a.toArray
//    return scala.collection.JavaConversions.asScalaBuffer(a).toArray
//  }

  override def keyBy[K](f: (T) => K): RDD[(K, T)] ={
    this.map(x => (f(x), x))
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  override protected def getPartitions: Array[Partition] = ???
}
object ScalaApexRDD extends {


}
