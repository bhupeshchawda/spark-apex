package com.datatorrent.example;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.SparkConf;

import java.io.Serializable;

@DefaultSerializer(JavaSerializer.class)
public class ApexConf extends SparkConf implements Serializable
{

  @Override
  public ApexConf setMaster(String master)
  {
    return (ApexConf) super.setMaster(master);
  }

  @Override
  public ApexConf setAppName(String name)
  {
    return (ApexConf) super.setAppName(name);
  }
}
