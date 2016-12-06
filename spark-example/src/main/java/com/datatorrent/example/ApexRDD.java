package com.datatorrent.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.LocalMode;
import com.datatorrent.example.utils.*;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import scala.Function1;
import scala.Function2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

import java.util.Random;

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
        return this.    dag;
    }

    public DefaultOutputPortSerializable getCurrentOutputPort(MyDAG cloneDag){

        try {
           MyBaseOperator currentOperator = (MyBaseOperator) cloneDag.getOperatorMeta(cloneDag.getLastOperatorName()).getOperator();
            return currentOperator.getOutputPort();
        } catch (Exception e) {
            System.out.println("Operator "+cloneDag.getLastOperatorName()+" Doesn't exist in the dag");
            e.printStackTrace();
        }
        return currentOperator.getOutputPort();
    }
    public DefaultOutputPortSerializable getControlOutput(MyDAG cloneDag){
        BaseInputOperator currentOperator= (BaseInputOperator) cloneDag.getOperatorMeta(cloneDag.getFirstOperatorName()).getOperator();
        return currentOperator.getControlOut();
    }
    @Override
    public <U> RDD<U> map(Function1<T, U> f, ClassTag<U> evidence$3) {

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new MapOperator());
        m1.f=f;
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream ", currentOutputPort, m1.input);
        this.dag = (MyDAG) SerializationUtils.clone(cloneDag);
        return (ApexRDD<U>) this;
    }

    @Override
    public RDD<T> filter(Function1<T, Object> f) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        FilterOperator filterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Filter", FilterOperator.class);
        filterOperator.f = f;
        cloneDag.addStream(System.currentTimeMillis()+ " FilterStream " + 1, currentOutputPort, filterOperator.input);
        this.dag = (MyDAG) SerializationUtils.clone(cloneDag);
        return this;
    }

    @Override
    public RDD<T> persist(StorageLevel newLevel) {
        return this;
    }
    public RDD<T>[] randomSplit(double[] weights){
        return randomSplit(weights, new Random().nextLong());
    }
    @Override
    public RDD<T>[] randomSplit(double[] weights, long seed){
        RDD<T>[] temp = super.randomSplit(weights, seed);
        for(RDD<T> t: temp){
            System.out.println((ApexRDD<T>)t);
        }
        return temp;
    }

    @Override
    public T reduce(Function2<T, T, T> f) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        ReduceOperator reduceOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Reduce " , new ReduceOperator());
        cloneDag.setInputPortAttribute(reduceOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        reduceOperator.f = f;

        Assert.assertTrue(currentOutputPort != null);
        cloneDag.addStream(System.currentTimeMillis()+" Reduce Input Stream", currentOutputPort, reduceOperator.input);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, reduceOperator.controlDone);

        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("target/tmp/outputData");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", reduceOperator.output, writer.input);

        System.out.println(cloneDag);
        cloneDag.validate();
        System.out.println("DAG successfully validated");

        LocalMode lma = LocalMode.newInstance();
        Configuration conf = new Configuration(false);
        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        try {
            lma.prepareDAG(app, conf);
        } catch (Exception e) {
            throw new RuntimeException("Exception in prepareDAG", e);
        }
        LocalMode.Controller lc = lma.getController();
        lc.run(30000);

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
