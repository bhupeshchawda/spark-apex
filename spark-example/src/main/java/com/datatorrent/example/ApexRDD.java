package com.datatorrent.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.LocalMode;
import com.datatorrent.example.scala.ApexRDDs;
import com.datatorrent.example.scala.ApexRDDs$;
import com.datatorrent.example.utils.*;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LabeledPoint$;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.tools.cmd.gen.AnyVals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import org.apache.spark.api.java.function.*;
public class ApexRDD<T> extends ApexRDDs<T> {
    private static final long serialVersionUID = -3545979419189338756L;

    public MyBaseOperator currentOperator;
    public OperatorType currentOperatorType;
    public DefaultOutputPortSerializable currentOutputPort;
    public DefaultOutputPortSerializable<Boolean> controlOutput;
    public  MyDAG dag;
    public ApexRDDPartitioner apexRDDPartitioner = new ApexRDDPartitioner();
    protected Option partitioner = new ApexRDDOptionPartitioner();
    public static ApexContext context;

    public ApexRDD(ApexContext ac) {
        super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
        this.context=ac;
        dag = new MyDAG();

    }
    public ApexRDD(RDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
        this.dag=((ApexRDD<T>)rdd).dag;
    }
    @Override
    public SparkContext context() {
        return context;
    }


    public RDD<Tuple2> combineByKey(Function1 createCombiner, Function2 mergeValue, Function2 mergeCombiners) {
        return combineByKey(createCombiner, mergeValue, mergeCombiners);
    }

    public RDD<Tuple2> combineByKey(Function1 createCombiner, Function2 mergeValue, Function2 mergeCombiners, int numPartitions) {
        return combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions);
    }

    public RDD<Tuple2> combineByKey(Function1 createCombiner, Function2 mergeValue, Function2 mergeCombiners, Partitioner partitioner, boolean mapSideCombine, Serializer serializer) {
        System.out.println("Combine By Key");
        return null;
    }

    public boolean combineByKey$default$5() {
        return false;
    }

    public Serializer combineByKey$default$6() {
        return null;
    }


    @Override
    public Option<Partitioner> partitioner() {

        return  new ApexRDDOptionPartitioner();
    }


    Logger log = LoggerFactory.getLogger(ApexRDD.class);
    public MyDAG getDag() {
        return this.dag;
    }

    public DefaultOutputPortSerializable getCurrentOutputPort(MyDAG cloneDag){

        try {
            log.info("Last operator in the Dag {}",dag.getLastOperatorName());
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
        cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        this.dag =  cloneDag;
        return (ApexRDD<U>) this;
    }
    public <U> RDD<U> map(Function<T, T> f) {

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapOperator1 m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map1 " , new MapOperator1());
        m1.ff=f;
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream1 ", currentOutputPort, m1.input);
        cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        this.dag =  cloneDag;
        return (ApexRDD<U>) this;
    }

    @Override
    public RDD<T> filter(Function1<T, Object> f) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        FilterOperator filterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Filter", FilterOperator.class);
        filterOperator.f = f;
        cloneDag.addStream(System.currentTimeMillis()+ " FilterStream " + 1, currentOutputPort, filterOperator.input);
        this.dag = cloneDag;

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
        writer.setAbsoluteFilePath("/tmp/outputData");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", reduceOperator.output, writer.input);

        cloneDag.validate();
        log.info("DAG successfully validated");

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
        lc.run(10000);
        Integer reduce = fileReader("/tmp/outputData");
        return (T) reduce;
    }

    public static Integer fileReader(String path){
        BufferedReader br = null;
        FileReader fr = null;
        try{
            fr = new FileReader(path);
            br = new BufferedReader(fr);
            String line;
            br = new BufferedReader(new FileReader(path));
            while((line = br.readLine())!=null){
                return Integer.valueOf(line);
            }
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }finally {
            try{
                if(br!=null)
                    br.close();
                if(fr!=null)
                    fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }



    @Override
    public Iterator<T> compute(Partition arg0, TaskContext arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Partition[] getPartitions() {
        // TODO Auto-generated method stub
        Partition[] partitions = new Partition[apexRDDPartitioner.numPartitions()];
        return partitions;
    }
    @Override
    public long count() {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);

        DefaultOutputPortSerializable currentCountOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        CountOperator countOperator = cloneDag.addOperator(System.currentTimeMillis()+ " CountOperator " , CountOperator.class);
        cloneDag.setInputPortAttribute(countOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Count Input Stream", currentCountOutputPort, countOperator.input);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, countOperator.controlDone);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/tmp/outputDataCount");
        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", countOperator.output, writer.input);
        cloneDag.validate();

        log.info("DAG successfully validated");
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
        lc.run(10000);

        Integer count = fileReader("/tmp/outputDataCount");
        return count;
    }

    @Override
    public T first() {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        FirstOpertaor firstOpertaor = cloneDag.addOperator(System.currentTimeMillis()+" FirstOperator",FirstOpertaor.class);
        cloneDag.setInputPortAttribute(firstOpertaor.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+"FirstTupleStream",currentOutputPort,firstOpertaor.input);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/tmp/outputDataFirst");
        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", firstOpertaor.output, writer.input);
        cloneDag.validate();
        log.info("DAG successfully validated");
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
        lc.run(10000);

        return (T) firstOpertaor.a;
    }

    @Override
    public ApexRDD<T>[] randomSplit(double[] weights, long seed){
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        MyDAG cloneDag2= (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);

        RandomSplitOperator randomSplitOperator = cloneDag.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        randomSplitOperator.weights=weights;
        cloneDag.setInputPortAttribute(randomSplitOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentOutputPort, randomSplitOperator.input);
        DefaultOutputPortSerializable currentSplitOutputPort2 = getCurrentOutputPort(cloneDag2);
        RandomSplitOperator randomSplitOperator2 = cloneDag2.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        randomSplitOperator2.weights=weights;
        randomSplitOperator2.flag=true;
        cloneDag2.setInputPortAttribute(randomSplitOperator2.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag2.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentSplitOutputPort2, randomSplitOperator2.input);
        ApexRDD temp1 =this;
        temp1.dag=cloneDag;
        ApexRDD temp2=this;
        temp2.dag=cloneDag2;
        ApexRDD[] temp=new ApexRDD[]{temp1, temp2};
        return temp;
    }

    @Override
    public T[] collect() {
        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        CollectOperator collectOperator =cloneDag.addOperator(System.currentTimeMillis()+" Collect Operator",CollectOperator.class);
        cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Collect Stream",currentOutputPort,collectOperator.input);
        cloneDag.validate();

        log.info("DAG successfully validated");
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
        lc.run(1000);
        T[] array= (T[]) CollectOperator.t.toArray();

        return array;
    }

    public enum OperatorType {
        INPUT,
        PROCESS,
        OUTPUT
    }
}
