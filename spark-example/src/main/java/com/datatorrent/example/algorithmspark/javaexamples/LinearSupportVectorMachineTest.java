package com.datatorrent.example.algorithmspark.javaexamples;

/**
 * Created by anurag on 27/12/16.
 */

import com.datatorrent.example.apexscala.AlgorithmTest;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

public class LinearSupportVectorMachineTest {
        public static void main(String[] args){
            SparkContext sc= new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Linear SVM"));
            String path = AlgorithmTest.diabetes();
            JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

// Split initial RDD into two... [60% training data, 40% testing data].
            JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
            JavaRDD<LabeledPoint> training = splits[0];
            JavaRDD<LabeledPoint> test = splits[1];

// Run training algorithm to build the model.
            int numIterations = 100;
            final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

// Clear the default threshold.
            model.clearThreshold();

// Compute raw scores on the test set.
            JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
                    new Function<LabeledPoint, Tuple2<Object, Object>>() {
                        public Tuple2<Object, Object> call(LabeledPoint p) {
                            Double score = model.predict(p.features());
                            return new Tuple2<Object, Object>(score, p.label());
                        }
                    }
            );

// Get evaluation metrics.
            BinaryClassificationMetrics metrics =
                    new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
            double auROC = metrics.areaUnderROC();

            System.out.println("Area under ROC = " + auROC);


// Save and load model
            model.save(sc, "target/tmp/javaSVMWithSGDModel");
            SVMModel sameModel = SVMModel.load(sc, "target/tmp/javaSVMWithSGDModel");
        }
}
