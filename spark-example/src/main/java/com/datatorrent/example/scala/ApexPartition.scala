package com.datatorrent.example.scala

import org.apache.spark.Partition

class ApexPartition extends Partition {
  override def index: Int = {
    return 0
  }

  override def equals(other: Any): Boolean = super.equals(other)
}
object ApexPartition{

}