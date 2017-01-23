package com.datatorrent.example;

import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.example.utils.BaseInputOperator;
import com.datatorrent.example.utils.MyBaseOperator;
import com.datatorrent.example.utils.SerializedInputSplit;
import com.datatorrent.example.utils.SplitRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.*;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import static com.datatorrent.api.Context.OperatorContext.PARTITIONER;
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
    SplitRecordReader fileInput = rdd.getDag().addOperator(System.currentTimeMillis()+ " Input ", SplitRecordReader.class);
    rdd.getDag().setAttribute(fileInput,PARTITIONER,new StatelessPartitioner<MyBaseOperator>(minPartitions));
    fileInput.minPartitions=minPartitions;
    rdd.currentOperator =  fileInput;
    rdd.currentOperatorType = ApexRDD.OperatorType.INPUT;
    rdd.currentOutputPort =  fileInput.output;
    rdd.controlOutput =fileInput.controlOut;
    fileInput.path = path;

    return rdd;
  }
  /*public InputSplit[] splitFileRecorder(String path, int minPartitions){
    Configuration conf = new Configuration(true);
    JobConf jobConf = new JobConf(conf);

    FileInputFormat fileInputFormat = new TextInputFormat();
    ((TextInputFormat)fileInputFormat).configure(jobConf);
    fileInputFormat.addInputPaths(jobConf,path);
    try {
      return  fileInputFormat.getSplits(jobConf,minPartitions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }*/

  @Override
  public <T> RDD<T> parallelize(Seq<T> seq, int numSlices, ClassTag<T> evidence$1) {
    return super.parallelize(seq, numSlices, evidence$1);
  }

}
