package com.datatorrent.example;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.example.utils.*;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.Operators;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;
import scala.Function1;
import scala.Function2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ApexRDD<T> extends RDD<T> {
    private static final long serialVersionUID = -3545979419189338756L;
    public MyBaseOperator currentOperator;
    public OperatorType currentOperatorType;
    public DefaultOutputPortSerializable<Object> currentOutputPort;
    public DefaultOutputPortSerializable<Boolean> controlOutput;
    private DAG dag;

    public ApexRDD(RDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
    }

    public ApexRDD(ApexContext ac) {
        super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
        dag = new LogicalPlan();

    }

    public DAG getDag() {
        return dag;
    }

    @Override
    public <U> RDD<U> map(Function1<T, U> f, ClassTag<U> evidence$3) {

        LogicalPlan cloneDag = (LogicalPlan) SerializationUtils.clone(dag);
        MapOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new MapOperator());
        System.out.println("CloneDag " + cloneDag.toString());
        System.out.println("Dag " + dag.toString());
        for (LogicalPlan.OperatorMeta o : cloneDag.getAllOperators()) {
            System.out.println("Clone Dag Operators \t "+ o.getOperator());
            currentOutputPort = ((MyBaseOperator) o.getOperator()).getOutputPort();
        }
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream ", currentOutputPort, m1.input);
        currentOutputPort = m1.output;
        currentOperator = m1;
//        ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
//        ObjectOutputStream out = null;
//        try {
//            out = new ObjectOutputStream(baos);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            out.writeObject(currentOutputPort);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        Assert.assertTrue(false);
        ApexRDD<U> temp = (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag = (LogicalPlan) SerializationUtils.clone(cloneDag);
        return temp;
    }

    @Override
    public RDD<T> filter(Function1<T, Object> f) {
        LogicalPlan cloneDag = (LogicalPlan) SerializationUtils.clone(dag);
        System.out.println("FILTER " + cloneDag.toString());
        FilterOperator filterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Filter", FilterOperator.class);
        filterOperator.f = f;

        for (LogicalPlan.OperatorMeta o : cloneDag.getAllOperators()) {
            System.out.println("Clone Dag Operators in Filter \t "+ o.getOperator());
            currentOutputPort = ((MyBaseOperator) o.getOperator()).getOutputPort();
        }

        cloneDag.addStream(System.currentTimeMillis()+ " FilterStream " + 1, currentOutputPort, filterOperator.input);
        currentOutputPort = filterOperator.output;
        currentOperator = filterOperator;
        System.out.println(currentOutputPort + "\t" + filterOperator.input);
        ApexRDD<T> temp = (ApexRDD<T>) SerializationUtils.clone(this);
        temp.dag = (LogicalPlan) SerializationUtils.clone(cloneDag);
        return temp;
    }


    @Override
    public T reduce(Function2<T, T, T> f) {
        LogicalPlan cloneDag = (LogicalPlan) SerializationUtils.clone(dag);
        ReduceOperator r1 = cloneDag.addOperator(System.currentTimeMillis()+ " Reduce " , ReduceOperator.class);
        FileWriterOperator fileWriterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " FileWriter ", FileWriterOperator.class);
        fileWriterOperator.setAbsoluteFilePath("/tmp/");
        try {
            if (r1.isControlInputOpen) {
                cloneDag.addStream(System.currentTimeMillis()+  " Control Done" , controlOutput, r1.controlDone);
                r1.isControlInputOpen = false;
            }
            if (r1.isInputPortOpen) {
                cloneDag.addStream(System.currentTimeMillis()+ " ReduceStream ", currentOutputPort, r1.input);
                r1.isInputPortOpen = false;
            }
            if (r1.isOutputPortOpen) {
                cloneDag.addStream(System.currentTimeMillis()+ " File Write Stream",r1.output, fileWriterOperator.input);
                r1.isOutputPortOpen = false;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        LocalMode lma = LocalMode.newInstance();
        Configuration conf = new Configuration(false);
        final LogicalPlan d = (LogicalPlan) getDag();

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