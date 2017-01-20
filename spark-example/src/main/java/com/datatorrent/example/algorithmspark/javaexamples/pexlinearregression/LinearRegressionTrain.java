package com.datatorrent.example.algorithmspark.javaexamples.pexlinearregression;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;

import alluxio.exception.AlluxioException;
import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import java.io.*;
import java.util.Properties;

/**
 * Created by krushika on 9/1/17.
 */
public class LinearRegressionTrain {

    public static void main(String[] args) throws IOException, AlluxioException {
        Properties properties = new Properties();
        InputStream input ;
        try{
            input = new FileInputStream("/home/krushika/dev/spark-apex/spark-example/src/main/java/com/datatorrent/example/properties/svm.properties");
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ApexConf conf = new ApexConf().setAppName("Linear Regression Example").setMaster("local");
        ApexContext sc = new ApexContext(conf);


        // Load and parse the data
        String path = properties.getProperty("LinearRegressionTrainData");
        ApexRDD<String> data = (ApexRDD<String>) sc.textFile(path,1);
        ApexRDD<LabeledPoint> parsedData = (ApexRDD<LabeledPoint>) data.map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");
                        String[] features = parts[1].split(" ");
                        double[] v = new double[features.length];
                        for (int i = 0; i < features.length - 1; i++)
                            v[i] = Double.parseDouble(features[i]);
                        return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
                    }
                }
        );

        // Building the model
        int numIterations = 100;
        final LinearRegressionModel model =
                LinearRegressionWithSGD.train(parsedData, numIterations);

//        FileSystem fs = FileSystem.Factory.get();
//        AlluxioURI pathAlluxio = new AlluxioURI("/PMMLModelLinear");
//        FileOutStream out = fs.createFile(pathAlluxio);
//        DataOutputStream ds = new DataOutputStream(out);
//        ds.writeChars(model.toPMML());
//        ds.close();
//        out.close();
        model.save(sc, properties.getProperty("LinearRegressionModelPath"));



    }
}
