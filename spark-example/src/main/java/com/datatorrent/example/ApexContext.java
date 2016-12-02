package com.datatorrent.example;

import com.datatorrent.example.utils.DefaultOutputPortSerializable;
import com.datatorrent.example.utils.FileReadOperator;
import com.datatorrent.example.utils.FileReaderOperator;
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
    FileReadOperator fileInput = rdd.getDag().addOperator("Input "+System.currentTimeMillis(), FileReadOperator.class);
    fileInput.setDirectory(path);
    fileInput.setPartitionCount(minPartitions);
    rdd.currentOperator = fileInput;
    rdd.currentOperatorType = ApexRDD.OperatorType.INPUT;
    rdd.currentOutputPort =  fileInput.output;
    rdd.controlOutput =fileInput.controlOut;
    System.out.println("Output Port "+rdd.currentOutputPort);
    return rdd;
  }
}
