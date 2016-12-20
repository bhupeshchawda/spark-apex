package com.datatorrent.example.utils

import org.apache.spark.Partition

class ApexPartition extends Partition {


  override def hashCode: Int = {
    return super.hashCode
  }

  def index: Int = {
    return 0
  }
}
