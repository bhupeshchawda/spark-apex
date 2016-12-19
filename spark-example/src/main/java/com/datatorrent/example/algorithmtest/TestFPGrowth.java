package com.datatorrent.example.algorithmtest;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.List;

/**
 * Created by krushika on 22/9/16.
 */
public class TestFPGrowth {

    public TestFPGrowth(ApexContext sc) {

        ApexRDD<String> data = (ApexRDD<String>) sc.textFile("/home/krushika/Spark-Apex/spark-example/src/main/resources/data/sample_fpgrowth.txt",1);
        ClassTag<String> fakeClassTag = ClassTag$.MODULE$.apply(String.class);

        ApexRDD<List<String>> transactions = (ApexRDD<List<String>>) data.map(
                (Function<String, List<String>>) line -> {
                    String[] parts = line.split(" ");
                    return Arrays.asList(parts);
                }
        );

        System.out.println("Transactions : "+transactions.toJavaRDD().collect()+"  "+transactions.count());

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.2)
                .setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions.toJavaRDD());


//        for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
//            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
//        }

//        double minConfidence = 0.8;
//        for (AssociationRules.Rule<String> rule
//                : model.generateAssociationRules(minConfidence).collect()) {
//            System.out.println(
//                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
//        }

    }
    public static void main(String [] args){

        ApexContext sc = new ApexContext(new ApexConf().setMaster("local").setAppName("TestAssociation"));
        TestFPGrowth testFPGrowth = new TestFPGrowth(sc);

    }
}


