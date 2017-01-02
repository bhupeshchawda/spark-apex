package com.datatorrent.example.algorithmapex;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * Created by anurag on 19/12/16.
 */
public class KMeans {
    public static void main(String args[]){
        ApexContext jsc= new ApexContext(new ApexConf().setMaster("local[2]").setAppName("Kmeans"));
        String path = "/home/anurag/spark-master/data/mllib/kmeans_data.txt";
        ApexRDD<String> data = (ApexRDD<String>) jsc.textFile(path,0);
        ApexRDD<Vector> parsedData = (ApexRDD<Vector>) data.map(
                new Function<String, Vector>() {
                    public Vector call(String s) {
                        String[] sarray = s.split(" ");
                        double[] values = new double[sarray.length];
                        for (int i = 0; i < sarray.length; i++) {
                            values[i] = Double.parseDouble(sarray[i]);
                        }
                        return Vectors.dense(values);
                    }
                }
        );
        parsedData.cache();

// Cluster the data into two classes using KMeans
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = org.apache.spark.mllib.clustering.KMeans.train(parsedData, numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(parsedData);
        System.out.println("Cost: " + cost);

// Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData);
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

// Save and load model
        clusters.save(jsc, "target/org/apache/spark/JavaKMeansExample/KMeansModel");
        KMeansModel sameModel = KMeansModel.load(jsc,
                "target/org/apache/spark/JavaKMeansExample/KMeansModel");
    }
}
