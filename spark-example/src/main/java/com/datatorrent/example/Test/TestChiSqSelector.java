package com.datatorrent.example.Test;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;
import scala.Function1;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;

/**
 * Created by harsh on 17/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class TestChiSqSelector implements Serializable {
    public static Function1 f;
    public TestChiSqSelector(){

    }
    public TestChiSqSelector(ApexContext sc){

        String path = "/home/harsh/apex-integration/spark-apex/spark-example/src/main/resources/data/sample_libsvm_data.txt";
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> inputData = new ApexRDD<LabeledPoint> (MLUtils.loadLibSVMFile(sc, path), tag);
        ChiSqSelector selector = new ChiSqSelector(50);
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
        System.out.println("before transformer");
        //Assert.assertTrue(discretizedData!=null);
        final ChiSqSelectorModel transformer = selector.fit(discretizedData);

    }
    public static void main(String args[]){
        ApexContext sc  = new ApexContext(new ApexConf().setMaster("local").setAppName("ApexApp_ChiSquare"));
        TestChiSqSelector t = new TestChiSqSelector(sc);
    }

}
