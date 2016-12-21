package com.datatorrent.example.algorithmtest;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
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
public class LogisticRegression {
    public static  void main(String [] args){
        ApexContext sc= new ApexContext(new ApexConf().setMaster("local[2]").setAppName("Kmeans"));
        String path = "/home/anurag/spark-master/data/mllib/sample_libsvm_data.txt";
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> data = new ApexRDD<>( MLUtils.loadLibSVMFile(sc, path),tag);

        // Split initial RDD into two... [60% training data, 40% testing data].
        ApexRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
        ApexRDD<LabeledPoint> training = splits[0];
        ApexRDD<LabeledPoint> test = splits[1];

        // Run training algorithm to build the model.
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(training);

        // Compute raw scores on the test set.
        ApexRDD<Tuple2<Object, Object>> predictionAndLabels = (ApexRDD<Tuple2<Object, Object>>) test.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);
        double accuracy = metrics.accuracy();
        System.out.println("Accuracy = " + accuracy);

// Save and load model
        model.save(sc, "target/tmp/javaLogisticRegressionWithLBFGSModel");
        LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc,
                "target/tmp/javaLogisticRegressionWithLBFGSModel");
    }

}
