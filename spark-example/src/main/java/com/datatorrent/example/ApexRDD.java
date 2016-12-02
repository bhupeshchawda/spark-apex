package com.datatorrent.example;

import com.datatorrent.api.*;
import com.datatorrent.example.utils.*;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Function2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.tools.nsc.transform.patmat.Logic;

public class ApexRDD<T> extends RDD<T>
{
  private static final long serialVersionUID = -3545979419189338756L;
  public   Operator currentOperator;
  public   OperatorType currentOperatorType;
  public DefaultOutputPort<Object> currentOutputPort;
  public  DefaultOutputPortSerializable<Boolean> controlOutput;
  private DAG dag;
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

    LogicalPlan cloneDag = (LogicalPlan) SerializationUtils.clone(dag);
    MapOperator m1 = cloneDag.addOperator("Map "+System.currentTimeMillis(), new MapOperator());


//
//    try {
//      if(m1.isInputPortOpen) {
//
//        m1.isInputPortOpen = false;
//        }
//        if(m1.isOutputPortOpen){
//          currentOutputPort=m1.output;
//          m1.isOutputPortOpen=false;
//        }
//
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    System.out.println("CloneDag "+ cloneDag.toString());
    System.out.println("Dag "+dag.toString());
    
//    Operator.OutputPort<Object> op = null;
//    System.out.println(cloneDag.getAllStreams().size());

    cloneDag.addStream("MapStream " + System.currentTimeMillis(), currentOutputPort, m1.input);
    System.out.println("MAP "+cloneDag.toString()+" \n"+currentOutputPort+" "+m1.input);
    currentOutputPort=m1.output;
    currentOperator=m1;
    ApexRDD<U> temp= (ApexRDD<U>) SerializationUtils.clone( this);
    temp.dag= (LogicalPlan) SerializationUtils.clone(cloneDag);
    return temp;
  }

  @Override
  public RDD<T> filter(Function1<T, Object> f)
  {
    LogicalPlan cloneDag = (LogicalPlan) SerializationUtils.clone(dag);
    System.out.println("FILTER "+cloneDag.toString());
    FilterOperator filterOperator = cloneDag.addOperator("Filter " + System.currentTimeMillis(), FilterOperator.class);
    filterOperator.f = f;
    try {
      if(filterOperator.isInputPortOpen) {


        filterOperator.isInputPortOpen = false;
        }
        if(filterOperator.isOutputPortOpen) {

          filterOperator.isOutputPortOpen=false;
        }

    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(currentOutputPort+"\t"+filterOperator.input);
    cloneDag.addStream("FilterStream " + System.currentTimeMillis() + 1, currentOutputPort, filterOperator.input);
    currentOutputPort = filterOperator.output;
    currentOperator=filterOperator;
    System.out.println(currentOutputPort+"\t"+filterOperator.input);
    ApexRDD<T> temp= (ApexRDD<T>) SerializationUtils.clone( this);
    temp.dag= (LogicalPlan) SerializationUtils.clone(cloneDag);
    return temp;
  }



  @Override
  public T reduce(Function2<T, T, T> f)
  {
    LogicalPlan cloneDag = (LogicalPlan) SerializationUtils.clone(dag);
    ReduceOperator r1 = cloneDag.addOperator("Reduce " + System.currentTimeMillis(), ReduceOperator.class);
    FileWriterOperator  fileWriterOperator = cloneDag.addOperator("FileWriter "+System.currentTimeMillis(),FileWriterOperator.class);
    fileWriterOperator.setAbsoluteFilePath("/tmp/");
    try {
      if(r1.isControlInputOpen){
        cloneDag.addStream("Control Done"+System.currentTimeMillis(), controlOutput,r1.controlDone);
        r1.isControlInputOpen=false;
      }
      if(r1.isInputPortOpen) {
        cloneDag.addStream("ReduceStream " + System.currentTimeMillis(), currentOutputPort, r1.input);
        r1.isInputPortOpen = false;
      }
        if(r1.isOutputPortOpen) {
          cloneDag.addStream("File Write Stream"+System.currentTimeMillis(),r1.output,fileWriterOperator.input);
          r1.isOutputPortOpen=false;
        }



    } catch (Exception e) {
      e.printStackTrace();
    }
    currentOperator =r1;
    r1.f = f;

    LocalMode lma=LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    final LogicalPlan d= (LogicalPlan) getDag();

    StreamingApplication app = new StreamingApplication() {
      public void populateDAG(DAG dag, Configuration conf) {
          for (LogicalPlan.OperatorMeta o:d.getAllOperators()){
              dag.addOperator(o.getName(),o.getOperator());
          }
        for (LogicalPlan.StreamMeta s : d.getAllStreams()) {
          for (LogicalPlan.InputPortMeta i : s.getSinks()) {
            Operator.OutputPort<Object> op = (Operator.OutputPort<Object>) s.getSource().getPortObject();
            Operator.InputPort<Object> ip = (Operator.InputPort<Object>) i.getPortObject();
            dag.addStream(s.getName(), op, ip);
          }
        }

      }
    };
    try {
      lma.prepareDAG(app,conf);
//      System.out.println("Dag Set in lma: " + lma.getDAG());
      ((LogicalPlan) lma.getDAG()).validate();

    } catch (Exception e) {
      e.printStackTrace();
    }
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.run(1000);
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
