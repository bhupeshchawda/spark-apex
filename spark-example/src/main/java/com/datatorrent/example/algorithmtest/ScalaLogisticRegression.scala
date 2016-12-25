package com.datatorrent.example.algorithmtest

/**
  * Created by anurag on 22/12/16.
  */
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
class ScalaLogisticRegression[T]{}
object ScalaLogisticRegression {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "/home/anurag/spark-apex/spark-example/src/main/resources/data/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

//    assert(false)
//    val summary=temp.treeAggregate(new MultivariateOnlineSummarizer)(
//      (aggregator, data) => aggregator.add(data),
//      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
//    new StandardScalerModel(
//      Vectors.dense(summary.variance.toArray.map(v => math.sqrt(v))),
//      summary.mean,
//      true,
//      false)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    // Save and load model
    model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    val sameModel = LogisticRegressionModel.load(sc,
      "target/tmp/scalaLogisticRegressionWithLBFGSModel")
  }
}
