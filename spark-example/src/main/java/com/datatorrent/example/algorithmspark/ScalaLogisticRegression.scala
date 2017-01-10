package com.datatorrent.example.algorithmspark

/**
  * Created by anurag on 22/12/16.
  */
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

class ScalaLogisticRegression[T]{}

object ScalaLogisticRegression {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    // Load training data in LIBSVM format.
    val path ="/home/anurag/spark-apex/spark-example/src/main/resources/data/diabetes.txt"
    val data2 = MLUtils.loadLibSVMFile(sc, path )

    // Split data into training (60%) and test (40%).
    val splits = data2.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

//    val data=training.map(_.features)
//
//    val summary=data.treeAggregate(new MultivariateOnlineSummarizer)(
//      (aggregator, data) => aggregator.add(data),
//      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
//    new StandardScalerModel(
//      Vectors.dense(summary.variance.toArray.map(v => math.sqrt(v))),
//      summary.mean,
//      true,
//      false)
//    println(summary.variance)
//    assert(false)
    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(data2)

    // Save and load model
    model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")

  }
}