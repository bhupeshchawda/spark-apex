package com.datatorrent.example.scala

import scala.{Function1, collection}
import scala.reflect.ClassTag

class IteratorFunction[T1, R] extends Function1[T1, R] with Serializable {


  override def compose[A](g: Function1[A, T1]): Function1[A, R] = {
    return null
  }

  override def andThen[A](g: Function1[R, A]): Function1[T1, A] = {
    return null
  }

  override def apply(v1: T1): R = ???
}


