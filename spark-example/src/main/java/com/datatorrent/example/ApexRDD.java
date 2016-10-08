package com.datatorrent.example;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
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

import java.lang.reflect.Field;

public class ApexRDD<T> extends RDD<T>
{
  private static final long serialVersionUID = -3545979419189338756L;

  public DAG dag;
  public Operator currentOperator;
  public OperatorType currentOperatorType;
  public DefaultOutputPort<Object> currentOutputPort;
  public DefaultInputPort<Object> currentInputPort;
  public Integer operatorCount;
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

    MapOperator m1 = dag.addOperator("Map "+System.currentTimeMillis(), MapOperator.class);

    try {
      if(m1.isInputPortOpen) {
        dag.addStream("MapStream " + System.currentTimeMillis(), currentOutputPort, m1.input);
        m1.isInputPortOpen=false;
        if(m1.isOutputPortOpen){
          currentOutputPort=m1.output;
          m1.isOutputPortOpen=false;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    currentOperator=m1;
    return (ApexRDD<U>)this;
  }

  @Override
  public RDD<T> filter(Function1<T, Object> f)
  {
    System.out.println(" map ");
    FilterOperator filterOperator = dag.addOperator("Filter " + System.currentTimeMillis(), FilterOperator.class);
    filterOperator.f = f;
    try {
      if(filterOperator.isInputPortOpen) {
        dag.addStream("MapStream " + System.currentTimeMillis(), currentOutputPort, filterOperator.input);
        filterOperator.isInputPortOpen=false;
        if(filterOperator.isOutputPortOpen) {
          currentOutputPort = filterOperator.output;
          filterOperator.isOutputPortOpen=false;
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    currentOperator=filterOperator;
    return this;
  }

  @Override
  public T reduce(Function2<T, T, T> f)
  {
    System.out.println(" map ");
    ReduceOperator r1 = dag.addOperator("Reduce " + System.currentTimeMillis(), ReduceOperator.class);
    try {
      if(r1.isInputPortOpen) {
        dag.addStream("MapStream " + System.currentTimeMillis(), currentOutputPort, r1.input);
        r1.isInputPortOpen=false;
        if(r1.isOutputPortOpen) {
          currentOutputPort = r1.output;
          r1.isOutputPortOpen=false;
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    currentOperator =r1;
    r1.f = f;
    System.out.println(dag.toString());
    return (T) new Integer(1);
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
