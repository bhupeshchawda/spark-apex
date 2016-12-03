package com.datatorrent.example;

import com.datatorrent.example.utils.BaseInputOperator;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

public class ApexContext extends SparkContext
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
    ApexRDD rdd = new ApexRDD<String>(this);
    BaseInputOperator fileInput = rdd.getDag().addOperator(System.currentTimeMillis()+ " Input ", BaseInputOperator.class);
   // fileInput.setDirectory(path);
    //fileInput.setPartitionCount(minPartitions);
    rdd.currentOperator =  fileInput;
    rdd.currentOperatorType = ApexRDD.OperatorType.INPUT;
    rdd.currentOutputPort =  fileInput.output;
    rdd.controlOutput =fileInput.controlOut;
    return rdd;
  }
}
