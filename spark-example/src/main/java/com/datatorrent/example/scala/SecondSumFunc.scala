package com.datatorrent.example.scala

import org.apache.spark.api.java.function.Function2

class SecondSumFunc[T1, T2, R] extends Function2[T1, T2, R] {
  @throws[Exception]
  override def call(v1: T1, v2: T2): R = {
    return (v1,v2).->(v1.toString + v2.toString).asInstanceOf[R]
  }
}

