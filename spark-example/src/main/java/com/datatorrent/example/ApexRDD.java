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
import org.jblas.util.Random;
import org.junit.Assert;
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
        ReduceOperator reduceOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Reduce " , new ReduceOperator());
        cloneDag.setInputPortAttribute(reduceOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        reduceOperator.f = f;

        Assert.assertTrue(this.currentOutputPort != null);
        cloneDag.addStream("S_"+System.currentTimeMillis()+ Random.nextInt(100), currentOutputPort, reduceOperator.input);
        cloneDag.addStream("S_"+System.currentTimeMillis()+Random.nextInt(100), controlOutput, reduceOperator.controlDone);

        FileWriterOperator writer = cloneDag.addOperator("Writer" + System.currentTimeMillis(), FileWriterOperator.class);
        cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/tmp/outputData");

        cloneDag.addStream("S_"+System.currentTimeMillis()+Random.nextInt(100), reduceOperator.output, writer.input);

        System.out.println(cloneDag);
        cloneDag.validate();
        System.out.println("DAG successfully validated");

        LocalMode lma = LocalMode.newInstance();
        Configuration conf = new Configuration(false);
//    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        try {
            lma.prepareDAG(app, conf);
        } catch (Exception e) {
            throw new RuntimeException("Exception in prepareDAG", e);
        }
        LocalMode.Controller lc = lma.getController();
        lc.run(10000);
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