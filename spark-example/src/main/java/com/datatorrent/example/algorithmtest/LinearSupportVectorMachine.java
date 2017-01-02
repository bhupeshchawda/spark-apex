package com.datatorrent.example.algorithmtest;

/**
 * Created by anurag on 27/12/16.
 */

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import com.datatorrent.example.apexscala.AlgorithmTest;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class LinearSupportVectorMachine{
    public static void main(String[] args){
        ApexContext sc= new ApexContext(new ApexConf().setMaster("local[2]").setAppName("Linear SVM"));
        String path = AlgorithmTest.data100();
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> data = new ApexRDD<>(MLUtils.loadLibSVMFile(sc, path),tag);

// Split initial RDD into two... [60% training data, 40% testing data].
        ApexRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
        ApexRDD<LabeledPoint> training = splits[0];
        ApexRDD<LabeledPoint> test = splits[1];

// Run training algorithm to build the model.
        int numIterations = 100;
        final SVMModel model = SVMWithSGD.train(training, numIterations);

// Clear the default threshold.
        model.clearThreshold();

// Compute raw scores on the test set.
        ApexRDD<Tuple2<Object, Object>> scoreAndLabels = (ApexRDD<Tuple2<Object, Object>>) test.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double score = model.predict(p.features());
                        return new Tuple2<Object, Object>(score, p.label());
                    }
                }
        );
        // Save and load model
        model.save(sc, "target/tmp/apexSVMWithSGDModel");

// Get evaluation metrics.
//        BinaryClassificationMetrics metrics =
//                new BinaryClassificationMetrics(scoreAndLabels);
//        double auROC = metrics.areaUnderROC();
//
//        System.out.println("Area under ROC = " + auROC);




    }
}
