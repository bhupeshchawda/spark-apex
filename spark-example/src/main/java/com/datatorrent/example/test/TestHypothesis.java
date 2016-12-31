package com.datatorrent.example.test;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.mllib.util.MLUtils;
import scala.Function1;
import scala.reflect.ClassTag;

import java.io.Serializable;

/**
 * Created by harsh on 31/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class TestHypothesis implements Serializable {
    public static Function1 f;
    public TestHypothesis(){}

    public TestHypothesis(ApexContext sc) {
        String path = "/home/harsh/apex-integration/spark-apex/spark-example/src/main/resources/data/sample_libsvm_data.txt";
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        Vector vec = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25);
        ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(vec);
        System.out.println(goodnessOfFitTestResult + "\n");
        Matrix mat = Matrices.dense(3, 2, new double[]{1.0, 3.0, 5.0, 2.0, 4.0, 6.0});
        ChiSqTestResult independenceTestResult = Statistics.chiSqTest(mat);
        System.out.println(independenceTestResult + "\n");
        ApexRDD<LabeledPoint> inputData = new ApexRDD<LabeledPoint> (MLUtils.loadLibSVMFile(sc, path), tag);
        ApexRDD discretizedData = (ApexRDD) inputData.map(new Function<LabeledPoint, LabeledPoint>() {
            @Override
            public LabeledPoint call(LabeledPoint lp) {
                final double[] discretizedFeatures = new double[lp.features().size()];
                for (int i = 0; i < lp.features().size(); ++i) {
                    discretizedFeatures[i] = Math.floor(lp.features().apply(i) / 16);
                }
                return new LabeledPoint(lp.label(), Vectors.dense(discretizedFeatures));
            }
        });
        ChiSqTestResult[] featureTestResults = Statistics.chiSqTest(discretizedData);
        int i = 1;
        for (ChiSqTestResult result : featureTestResults) {
            System.out.println("Column " + i + ":");
            System.out.println(result + "\n");  // summary of the test
            i++;
        }
    }

    public static void main(String args[]){
        ApexContext sc  = new ApexContext(new ApexConf().setMaster("local").setAppName("ApexApp_Hypothesis"));
        TestHypothesis t = new TestHypothesis(sc);
    }
}
