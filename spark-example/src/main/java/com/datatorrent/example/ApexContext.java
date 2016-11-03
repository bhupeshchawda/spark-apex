package com.datatorrent.example;

import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.example.utils.FileReaderOperator;
import com.datatorrent.example.utils.JavaSerializationStreamCodec;

public class ApexContext extends SparkContext
{
  private static Logger LOG = LoggerFactory.getLogger(ApexContext.class);

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
    LOG.info("Adding File Input Operator");
    FileReaderOperator fileInput = rdd.getDag().addOperator("Input"+System.currentTimeMillis(), FileReaderOperator.class);
    fileInput.setDirectory(path);
    fileInput.setPartitionCount(minPartitions);
    rdd.currentOutputPort = fileInput.output;
    rdd.controlOutputPort = fileInput.controlOut;
    return rdd;
  }
}
