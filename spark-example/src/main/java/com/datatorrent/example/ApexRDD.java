package com.datatorrent.example;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.example.utils.FilterOperator;
import com.datatorrent.example.utils.MapOperator;
import com.datatorrent.example.utils.ReduceOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import scala.Function1;
import scala.Function2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

public class ApexRDD<T> extends RDD<T>
{
  private static final long serialVersionUID = -3545979419189338756L;

  public DAG dag;
  public Operator currentOperator;
  public OperatorType currentOperatorType;

  public ApexRDD(RDD<T> rdd, ClassTag<T> classTag)
  {
    super(rdd, classTag);
  }

  public ApexRDD(ApexContext ac)
  {
    super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
    dag = new LogicalPlan();
  }

  public DAG getDag()
  {
    return dag;
  }

  @Override
  public <U> RDD<U> map(Function1<T, U> f, ClassTag<U> evidence$3)
  {
    MapOperator m1 = dag.addOperator("Map"+System.currentTimeMillis(), MapOperator.class);
    m1.f = f;
    return (ApexRDD<U>)this;
  }

  @Override
  public RDD<T> filter(Function1<T, Object> f)
  {
    FilterOperator m1 = dag.addOperator("Map" + System.currentTimeMillis(), FilterOperator.class);
    m1.f = f;
    return this;
  }

  @Override
  public T reduce(Function2<T, T, T> f)
  {
    ReduceOperator r1 = dag.addOperator("Reduce" + System.currentTimeMillis(), ReduceOperator.class);
    r1.f = f;
    return (T) this;
  }

  @Override
  public Iterator<T> compute(Partition arg0, TaskContext arg1)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition[] getPartitions()
  {
    // TODO Auto-generated method stub
    return null;
  }

  public enum OperatorType {
    INPUT,
    PROCESS,
    OUTPUT
  }
}
