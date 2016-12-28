package com.datatorrent.example.apexscala

import com.datatorrent.example.ApexRDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.model.DecisionTreeModel

import scala.collection.JavaConverters._
/**
  * Created by anurag on 28/12/16.
  */
class DecisionTreeApex(val strategy: Strategy ) extends DecisionTree(strategy ){


}
object DecisionTreeApex{
  def trainClassifier(
                       input: ApexRDD[LabeledPoint],
                       numClasses: Int,
                       categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
                       impurity: String,
                       maxDepth: Int,
                       maxBins: Int): DecisionTreeModel = {

      DecisionTree.trainClassifier(input, numClasses,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      impurity, maxDepth, maxBins)
  }
}
