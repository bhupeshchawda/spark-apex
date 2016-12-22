package com.datatorrent.example.apexscala

import org.apache.spark.Partition

/**
  * Created by anurag on 14/12/16.
  */

class ApexPartition extends Partition with Serializable{
  def index = 0

  override def hashCode: Int = super.hashCode

  override def equals(other: Any): Boolean = super.equals(other)
}
