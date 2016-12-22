package com.datatorrent.example;

import com.datatorrent.example.utils.BaseInputOperator;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;

//@DefaultSerializer(JavaSerializer.class)
public class ApexContext extends SparkContext implements Serializable
{
  public ApexContext()
  {
    super(new ApexConf());
  }

  public ApexContext(ApexConf config)
  {
    super(config);
  }

  @Override
  public RDD<String> textFile(String path, int minPartitions)
  {
    ApexRDD rdd = new ApexRDD<>(this);
    BaseInputOperator fileInput = rdd.getDag().addOperator(System.currentTimeMillis()+ " Input ", BaseInputOperator.class);
    rdd.currentOperator =  fileInput;
    rdd.currentOperatorType = ApexRDD.OperatorType.INPUT;
    rdd.currentOutputPort =  fileInput.output;
    rdd.controlOutput =fileInput.controlOut;
    fileInput.path = path;

    return rdd;
  }
}
