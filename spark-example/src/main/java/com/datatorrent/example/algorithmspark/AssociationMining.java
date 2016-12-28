package com.datatorrent.example.algorithmspark;

/**
 * Created by anurag on 27/12/16.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;

import java.util.Arrays;
public class AssociationMining {
    public static void main(String [] args){


        SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTreeClassificationExample");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = sc.parallelize(Arrays.asList(
                new FreqItemset<String>(new String[] {"a"}, 15L),
                new FreqItemset<String>(new String[] {"b"}, 35L),
                new FreqItemset<String>(new String[] {"a", "b"}, 12L)
        ));

        AssociationRules arules = new AssociationRules()
                .setMinConfidence(0.8);
        JavaRDD<AssociationRules.Rule<String>> results = arules.run(freqItemsets);

        for (AssociationRules.Rule<String> rule : results.collect()) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }
    }
}
