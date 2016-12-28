package com.datatorrent.example.algorithmtest;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import com.datatorrent.example.apexscala.DecisionTreeApex;
import com.datatorrent.example.apexscala.Test;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.reflect.ClassTag;

import java.util.HashMap;

/**
 * Created by anurag on 27/12/16.
 */
public class DecisionTreeJava {
    public static void main(String args[]) {
        ApexConf apexConf = new ApexConf().setMaster("local[2]").setAppName("JavaDecisionTreeClassificationExample");
        ApexContext ac = new ApexContext(apexConf);
        // Load and parse the data file.
        String datapath = Test.data100();
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> data = new ApexRDD<>(MLUtils.loadLibSVMFile(ac, datapath),tag);
// Split the data into training and test sets (30% held out for testing)
        ApexRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3},11L);
        ApexRDD<LabeledPoint> trainingData = splits[0];
        ApexRDD<LabeledPoint> testData = splits[1];

// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;

// Train a DecisionTreeJava model for classification.
        final DecisionTreeModel model = DecisionTreeApex.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Save and load model
        model.save(ac, "target/tmp/myDecisionTreeClassificationModel");
        DecisionTreeModel sameModel = DecisionTreeModel
                .load(ac, "target/tmp/myDecisionTreeClassificationModel");
//// Evaluate model on test instances and compute test error
//        JavaPairRDD<Double, Double> predictionAndLabel =
//                testData.map(new PairFunction<LabeledPoint, Double, Double>() {
//                    @Override
//                    public Tuple2<Double, Double> call(LabeledPoint p) {
//                        return new Tuple2<>(model.predict(p.features()), p.label());
//                    }
//                });
//        Double testErr =
//                1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
//                    @Override
//                    public Boolean call(Tuple2<Double, Double> pl) {
//                        return !pl._1().equals(pl._2());
//                    }
//                }).count() / testData.count();
//
//        System.out.println("Test Error: " + testErr);
//        System.out.println("Learned classification tree model:\n" + model.toDebugString());


    }
}
