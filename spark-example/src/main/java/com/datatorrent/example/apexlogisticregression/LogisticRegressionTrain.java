package com.datatorrent.example.apexlogisticregression;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.reflect.ClassTag;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by krushika on 5/1/17.
 */
public class LogisticRegressionTrain {
    public static void main(String[] args){
        Properties properties = new Properties();
        InputStream input ;
        try{
            input = new FileInputStream(properties.getProperty("/home/krushika/dev/spark-apex/spark-example/src/main/java/com/datatorrent/example/properties/svm.properties"));
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ApexContext sc= new ApexContext(new ApexConf().setMaster("local[2]").setAppName("Logistic Regression Train Module"));
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> data = new ApexRDD<>( MLUtils.loadLibSVMFile(sc,properties.getProperty("trainData")),tag);

        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(data);

        model.save(sc, properties.getProperty("LogisticRegressionModelPath"));


    }

}
