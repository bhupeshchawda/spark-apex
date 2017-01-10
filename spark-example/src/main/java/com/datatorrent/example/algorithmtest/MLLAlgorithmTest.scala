package com.datatorrent.example.algorithmtest

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by anurag on 16/12/16.
  */
//class Greeter(message: String, secondaryMessage: String) {
//  def this(message: String) = {
//    this(message, "")
//    println(message)
//  }
//
//  def SayHi() = println(message + secondaryMessage)
//}
//class A(val n: Int)
//object A {
//  implicit def str(a: A): String = "A: %d" format a.n
//}
//class B(val x: Int, y: Int) extends A(y)
//object B {
//   implicit def add(a: A): String= "A :%d" format a.n
//}
//
//class X(val i:Int) {
//  def add(implicit x:X)=println(x.i+i)
//}
//
//object X {
//  implicit def xx = new X(3)
//}


object MLlibAlgorithmTest {
  var data100= ""
  var diabetes = "/home/anurag/dev/spark-apex/spark-example/src/main/resources/data/diabetes.txt"
//  def setProperties(): Unit ={
//    val prop = new Properties()
//    prop.load(new FileInputStream("/home/anurag/dev/spark-apex/spark-example/src/main/resources/path.properties"))
//    data100=prop.getProperty(data100)
//    diabetes=prop.getProperty(diabetes)
//  }

  def testLogisticRegression(sc:SparkContext): (Double,Double) ={

    val data2 = MLUtils.loadLibSVMFile(sc, diabetes )
    // Split data into training (60%) and test (40%).
    val splits = data2.randomSplit(Array(0.6, 0.4))
    val training = splits(0).cache()
    val test = splits(1)
    val apexModel = LogisticRegressionModel.load(sc, "target/tmp/apexLogisticRegressionWithLBFGSModel")
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = apexModel.predict(features)
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracyApexModel=metrics.accuracy


    val sparkModel = LogisticRegressionModel.load(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    val predictionAndLabelsSpark = test.map { case LabeledPoint(label, features) =>
      val prediction = sparkModel.predict(features)
      (prediction, label)
    }
    val metrics2 = new MulticlassMetrics(predictionAndLabelsSpark)
    val accuracySparkModel=metrics2.accuracy
    println("Apex Accuracy "+accuracyApexModel)
    println("Spark Accuracy "+accuracySparkModel)
    (accuracyApexModel,accuracySparkModel)

  }
  def testLinearSVM(sc:SparkContext): Unit ={
    import org.apache.spark.mllib.classification.SVMModel
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    import org.apache.spark.mllib.util.MLUtils

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, diabetes)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4))
    val training = splits(0).cache()
    val test = splits(1)
    //Testing Spark Model
    val sparkModel = SVMModel.load(sc, "target/tmp/javaSVMWithSGDModel")
    val scoreAndLabels = test.map { point =>
      val score = sparkModel.predict(point.features)
      (score, point.label)
    }
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROCSpark = metrics.areaUnderROC()
    //Testing Apex Model
    val apexModel = SVMModel.load(sc, "target/tmp/apexSVMWithSGDModel")
    val scoreAndLabelsApex = test.map { point =>
      val score = apexModel.predict(point.features)
      (score, point.label)
    }
    // Get evaluation metrics2.
    val metrics2 = new BinaryClassificationMetrics(scoreAndLabelsApex)
    val auROCApex = metrics2.areaUnderROC()
    println("Spark:- Area under ROC = " + auROCSpark)
    println("Apex:- Area under ROC = " + auROCApex)
  }
  def testLinearRegression(sc:SparkContext): Unit ={
    val data = sc.textFile("/home/krushika/dev/spark-apex/spark-example/src/main/resources/data/lspaTest.txt")
    val parsedData=data.map{line=>
    val parts=line.split(',')
    LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    val model=regression.LinearRegressionModel.load(sc,"/home/krushika/dev/spark-apex/spark-example/target/tmp/apexLinearModel")
    val valuesAndPreds = parsedData.map{ point =>
    val prediction = model.predict(point.features)
      (point.label,prediction)
    }
    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("training Mean Squared Error = " + MSE)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AlgorithmTEST").setMaster("local")
    val sc = new SparkContext(conf)
    val data=new ArrayBuffer[(Double,Double)]
//    for(a<- 1 to 50)
    println(testLogisticRegression(sc))
    println(data)
  }
  //    Logistic Regression
  //    Apex model accuracy 0.6622516556291391
  //    Spark model accuracy 0.6986754966887417

  //    LinearSVM
  //    Spark:- Area under ROC = 0.7174120795107028
  //    Apex:- Area under ROC = 0.5

}
