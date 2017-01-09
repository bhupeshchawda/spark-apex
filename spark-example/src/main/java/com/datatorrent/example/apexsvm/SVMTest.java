package com.datatorrent.example.apexsvm;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import com.datatorrent.example.apexscala.AlgorithmTest;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.lib.SVDPlusPlus;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by krushika on 5/1/17.
 */
public class SVMTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        InputStream input;
        try {
            input = new FileInputStream("/home/krushika/dev/spark-apex/spark-example/src/main/java/com/datatorrent/example/properties/svm.properties");
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        SparkContext sc= new SparkContext(new SparkConf().setMaster("local").setAppName("Linear SVM Testing Module"));
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        JavaRDD<LabeledPoint> data = new JavaRDD<>(MLUtils.loadLibSVMFile(sc, properties.getProperty("testData")),tag);

        final SVMModel model = SVMModel.load(sc, properties.getProperty("SVMModelPath"));

        JavaRDD<Tuple2<Object, Object>> scoreAndLabels =  data.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double score = model.predict(p.features());
                        return new Tuple2<Object, Object>(score, p.label());
                    }
                }
        );

        BinaryClassificationMetrics metrics2 = new BinaryClassificationMetrics(scoreAndLabels.rdd());
        double apexROC = metrics2.areaUnderROC();
        System.out.println("Area under ROC :"+apexROC);
    }
}
