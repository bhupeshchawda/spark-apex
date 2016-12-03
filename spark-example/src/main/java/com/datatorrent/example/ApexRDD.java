package com.datatorrent.example;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.example.utils.*;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Function2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

public class ApexRDD<T> extends RDD<T> {
    private static final long serialVersionUID = -3545979419189338756L;
    public MyBaseOperator currentOperator;
    public OperatorType currentOperatorType;
    public DefaultOutputPortSerializable currentOutputPort;
    public DefaultOutputPortSerializable<Boolean> controlOutput;
    private MyDAG dag;

    public ApexRDD(RDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
    }

    public ApexRDD(ApexContext ac) {
        super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
        dag = new MyDAG();

    }

    public MyDAG getDag() {
        return dag;
    }

    public DefaultOutputPortSerializable getCurrentOutputPort(MyDAG cloneDag){
        currentOperator= (MyBaseOperator) cloneDag.getOperatorMeta(cloneDag.getLastOperatorName()).getOperator();
        return currentOperator.getOutputPort();
    }
    public DefaultOutputPortSerializable getControlOutput(MyDAG cloneDag){
        currentOperator= (MyBaseOperator) cloneDag.getOperatorMeta(cloneDag.getFirstOperatorName()).getOperator();
        return currentOperator.getControlOut();
    }
    @Override
    public <U> RDD<U> map(Function1<T, U> f, ClassTag<U> evidence$3) {

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        currentOutputPort= getCurrentOutputPort(cloneDag);
        MapOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new MapOperator());
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream ", currentOutputPort, m1.input);
        currentOutputPort = m1.output;
//        currentOperator = m1;
        ApexRDD<U> temp = (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag = (MyDAG) SerializationUtils.clone(cloneDag);
        return temp;
    }

    @Override
    public RDD<T> filter(Function1<T, Object> f) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        currentOutputPort=getCurrentOutputPort(cloneDag);
        FilterOperator filterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Filter", FilterOperator.class);
        filterOperator.f = f;
        cloneDag.addStream(System.currentTimeMillis()+ " FilterStream " + 1, currentOutputPort, filterOperator.input);
        currentOutputPort = filterOperator.output;
//        currentOperator = filterOperator;
        ApexRDD<T> temp = (ApexRDD<T>) SerializationUtils.clone(this);
        temp.dag = (MyDAG) SerializationUtils.clone(cloneDag);
        return temp;
    }

    @Override
    public RDD<T> persist(StorageLevel newLevel) {
        return this;
    }

    @Override
    public T reduce(Function2<T, T, T> f) {
        System.out.println("We are in reduce");
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        currentOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        ReduceOperator r1 = cloneDag.addOperator(System.currentTimeMillis()+ " Reduce " , ReduceOperator.class);
        FileWriterOperator fileWriterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " FileWriter ", FileWriterOperator.class);
        fileWriterOperator.setAbsoluteFilePath("/tmp/");

        try {
            if (r1.isControlInputOpen) {
                System.out.println("We are in reduce1");
                cloneDag.addStream(System.currentTimeMillis()+  " Control Done" , controlOutput, r1.controlDone);
                r1.isControlInputOpen = false;
            }
            if (r1.isInputPortOpen) {
                System.out.println("We are in reduce2");
                cloneDag.addStream(System.currentTimeMillis()+ " ReduceStream ", currentOutputPort, r1.input);
                r1.isInputPortOpen = false;
            }
            if (r1.isOutputPortOpen) {
                System.out.println("We are in reduce3");
                cloneDag.addStream(System.currentTimeMillis()+ " File Write Stream",r1.output, fileWriterOperator.input);
                r1.isOutputPortOpen = false;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        LocalMode lma = LocalMode.newInstance();
        Configuration conf = new Configuration(false);
        final LogicalPlan d = (MyDAG) getDag();

        StreamingApplication app = new StreamingApplication() {
            public void populateDAG(DAG dag, Configuration conf) {
                for (LogicalPlan.OperatorMeta o : d.getAllOperators()) {
                    dag.addOperator(o.getName(), o.getOperator());
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
            lma.prepareDAG(app, conf);
            System.out.println("Dag Set in lma: " + lma.getDAG());
            ((LogicalPlan)lma.getDAG()).validate(); //(MyDAG)

        } catch (Exception e) {
            e.printStackTrace();
        }
        LocalMode.Controller lc = lma.getController();
        lc.setHeartbeatMonitoringEnabled(false);

        lc.run(1000);
        return (T) new Integer(1);
    }

    @Override
    public Iterator<T> compute(Partition arg0, TaskContext arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Partition[] getPartitions() {
        // TODO Auto-generated method stub
        return null;
    }

    public enum OperatorType {
        INPUT,
        PROCESS,
        OUTPUT
    }
}
 /*   try {
      if(filterOperator.isInputPortOpen) {


        filterOperator.isInputPortOpen = false;
        }
        if(filterOperator.isOutputPortOpen) {

          filterOperator.isOutputPortOpen=false;
        }

    } catch (Exception e) {
      e.printStackTrace();
    }*/