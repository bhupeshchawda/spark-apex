package com.datatorrent.example;


import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.reflect.ClassTag;

import java.io.Serializable;

@DefaultSerializer(JavaSerializer.class)
public class TestNaiveBayes implements Serializable
{

  public TestNaiveBayes()
  {
    // TODO Auto-generated constructor stub
  }
  public TestNaiveBayes(ApexContext sc)
  {

    String path = "/home/anurag/dev/spark-apex/spark-example/src/main/resources/data/sample_libsvm_data.txt";
    ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
    ApexRDD<LabeledPoint> inputData = new ApexRDD<LabeledPoint> (MLUtils.loadLibSVMFile(sc, path), tag);
    System.out.println("Count: " + inputData.count());
    ApexRDD<LabeledPoint>[] tmp = (ApexRDD<LabeledPoint>[]) inputData.randomSplit(new double[]{0.6, 0.4});
//    System.out.println(Arrays.toString(tmp[0].collect()));
//    System.out.println(tmp[1].count());
    ApexRDD<LabeledPoint> training =tmp[0]; // training set
//      Assert.assertTrue(training!=null);
//    ApexRDD<LabeledPoint> test = tmp[1]; // getCurrentOutputPort set
    final NaiveBayesModel model = NaiveBayes.train(training, 1.0);
//    JavaPairRDD<Double, Double> predictionAndLabel =
//        getCurrentOutputPort.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
//          @Override
//          public Tuple2<Double, Double> call(LabeledPoint p) {
//            return new Tuple2<>(model.predict(p.features()), p.label());
//          }
//        });
//      double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
//        @Override
//        public Boolean call(Tuple2<Double, Double> pl) {
//          return pl._1().equals(pl._2());
//        }
//      }).count() / (double) getCurrentOutputPort.count();
//
//      // Save and load model
//      model.save(sc.sc(), "target/tmp/myNaiveBayesModel");
//      NaiveBayesModel sameModel = NaiveBayesModel.load(sc.sc(), "target/tmp/myNaiveBayesModel");
//      System.out.println("Accuracy: " + accuracy);
  }

  public static void main(String[] args)
  {
//    JavaSparkContext sc  = new JavaSparkContext(new SparkConf().setMaster("local[2]").setAppName("TestNaiveBayes"));
    ApexContext sc  = new ApexContext(new ApexConf().setMaster("local[2]").setAppName("ApexApp"));
    TestNaiveBayes t = new TestNaiveBayes(sc);
  }
}
