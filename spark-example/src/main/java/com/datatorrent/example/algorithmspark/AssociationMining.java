package com.datatorrent.example.algorithmspark;

/**
 * Created by anurag on 27/12/16.
 */
// $example on$

import com.datatorrent.example.ApexRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import scala.reflect.ClassTag;

import java.util.Arrays;

// $example off$

public class AssociationMining {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaAssociationRulesExample");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        ClassTag<FreqItemset<String>> tag = scala.reflect.ClassTag$.MODULE$.apply(FreqItemset.class);
        ClassTag<AssociationRules.Rule<String>> tagRuleString = scala.reflect.ClassTag$.MODULE$.apply(AssociationRules.Rule.class);
        ClassTag<String> tagString = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        // $example on$
        JavaRDD<FreqItemset<String>> d = sc.parallelize(Arrays.asList(
                new FreqItemset<String>(new String[]{"a"}, 15L),
                new FreqItemset<String>(new String[]{"b"}, 35L),
                new FreqItemset<String>(new String[]{"a", "b"}, 12L)
        ));
        ApexRDD<FreqItemset<String>> freqItemsets = new ApexRDD<>(d.rdd(),tag);

        AssociationRules arules = new AssociationRules()
                .setMinConfidence(0.8);
        ApexRDD<AssociationRules.Rule<String>> results = new ApexRDD<AssociationRules.Rule<String>>(arules.run(freqItemsets,tagString),tagRuleString);

        for (AssociationRules.Rule<String> rule : results.collect()) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }
        // $example off$

        sc.stop();
    }
}
