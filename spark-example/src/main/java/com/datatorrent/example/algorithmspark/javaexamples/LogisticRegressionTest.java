package com.datatorrent.example.algorithmspark.javaexamples;

import com.datatorrent.example.apexscala.AlgorithmTest;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * Created by anurag on 19/12/16.
 */
public class    LogisticRegressionTest {
    public static  void main(String [] args){
        SparkContext sc= new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Kmeans"));
        String path = "/home/anurag/spark-master/data/mllib/sample_libsvm_data.txt";
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        JavaRDD<LabeledPoint> data = new JavaRDD<>( MLUtils.loadLibSVMFile(sc,AlgorithmTest.diabetes()),tag);

        // Split initial RDD into two... [60% training data, 40% testing data].
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
        JavaRDD<LabeledPoint> training = splits[0];
        JavaRDD<LabeledPoint> test = splits[1];

        // Run training algorithm to build the model.
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(training.rdd());

        // Compute raw scores on the test set.
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double accuracy = metrics.accuracy();
        System.out.println("Accuracy = " + accuracy);

// Save and load model
        model.save(sc, "target/tmp/JAVALogisticRegressionWithLBFGSModel");
        LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc,
                "target/tmp/javaLogisticRegressionWithLBFGSModel");
    }

}
