package com.datatorrent.example;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.example.apexscala.ApexPartition;
import com.datatorrent.example.apexscala.ScalaApexRDD;
import com.datatorrent.example.utils.*;
import com.datatorrent.stram.client.StramAppLauncher;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.Function2;
import scala.Option;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;

public class ApexRDD<T> extends ScalaApexRDD<T> implements Serializable {
    private static final long serialVersionUID = -3545979419189338756L;
    public static ApexContext context;
    public static ApexContext _sc;
    private static PairRDDFunctions temp;
    public MyBaseOperator currentOperator;
    public OperatorType currentOperatorType;
    public DefaultOutputPortSerializable currentOutputPort;
    public DefaultOutputPortSerializable controlOutput;
    public  MyDAG dag;
    public ApexRDDPartitioner apexRDDPartitioner = new ApexRDDPartitioner();
    public Partition[] partitions_=getPartitions();
    protected Option<Partitioner> partitioner = (Option<Partitioner>) new ApexRDDOptionPartitioner();
    Logger log = LoggerFactory.getLogger(ApexRDD.class);



    public ApexRDD(RDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
        this.dag=((ApexRDD<T>)rdd).dag;

    }

    public ApexRDD(ApexContext ac) {
        super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
//        super.setSparkContext(context);
        dag = new MyDAG();
        context=ac;
        _sc=ac;
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
    public SparkContext sparkContext() {
        return context;
    }

    @Override
    public Option<Partitioner> partitioner() {
        return new ApexRDDOptionPartitioner();
    }

    @Override
    public SparkContext context() {
        return context;
    }

    public MyDAG getDag() {
        return this.dag;
    }

    public DefaultOutputPortSerializable getCurrentOutputPort(MyDAG cloneDag){

        try {
            log.debug("Last operator in the Dag {}",dag.getLastOperatorName());
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

    public <U> RDD<U> map(Function <T,U> f){

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapOperatorFunction m1 = cloneDag.addOperator(System.currentTimeMillis()+ " MapFunction " , new MapOperatorFunction());
        m1.f=context.clean(f,true);

//        ScalaApexRDD$.MODULE$.test((ScalaApexRDD<Tuple2<Object, Object>>) this, (ClassTag<Object>) evidence$3,null,null);
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream Function", currentOutputPort, m1.input);
       // cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<U> temp= (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag=cloneDag;
        return temp;
    }

    @Override
    public T first() {
        return this.collect()[0];
    }

    @Override
    public T[] collect() {
        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        CollectOperator collectOperator =cloneDag.addOperator(System.currentTimeMillis()+" Collect Operator",CollectOperator.class);

//        cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Collect Stream",currentOutputPort,collectOperator.input);
        try {
            runDag(cloneDag,3000,"collect");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        T[] array= (T[]) CollectOperator.t.toArray();

        return array;
    }

    @Override
    public T[] take(int num) {
        ArrayList<T> a = new ArrayList<>(num);
        Object[] ret = this.collect();
        for ( int i=0; i< num; i++) {
            a.add((T) ret[i]);
        }
    return a.toArray((T[]) Array.newInstance(this.getClass().getTypeParameters()[0].getClass(), num));
    }

    @Override
    public <U> RDD<U> map(Function1<T, U> f, ClassTag<U> evidence$3) {

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new MapOperator());
        m1.f= f;
//        ScalaApexRDD$.MODULE$.test((ScalaApexRDD<Tuple2<Object, Object>>) this, (ClassTag<Object>) evidence$3,null,null);
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream ", currentOutputPort, m1.input);
        //cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<U> temp= (ApexRDD<U>) createClone(cloneDag);
        return temp;
    }

    @Override
    public RDD<T> filter(Function1<T, Object> f) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        FilterOperator filterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Filter", FilterOperator.class);
        filterOperator.f = context.clean(f,true);
        cloneDag.addStream(System.currentTimeMillis()+ " FilterStream " + 1, currentOutputPort, filterOperator.input);
        return createClone(cloneDag);
    }

    @Override
    public RDD<T> persist(StorageLevel newLevel) {
        return this;
    }

    @Override
    public RDD<T> unpersist(boolean blocking) {
        return this;
    }

    public RDD<T>[] randomSplit(double[] weights){
        return randomSplit(weights, new Random().nextLong());
    }

    @Override
    public <U> RDD<U> mapPartitions(Function1<Iterator<T>, Iterator<U>> f, boolean preservesPartitioning, ClassTag<U> evidence$6) {

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput=getControlOutput(cloneDag);
        MapPartitionOperator mapPartitionOperator= cloneDag.addOperator(System.currentTimeMillis()+ " MapPartition " , new MapPartitionOperator());
        mapPartitionOperator.f = f;
        cloneDag.addStream( System.currentTimeMillis()+ " MapPartitionStream ", currentOutputPort, mapPartitionOperator.input);
       // cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<U> temp= (ApexRDD<U>) createClone(cloneDag);
        return temp;
    }

//
//    @Override
//    public T[] take(int num) {
//        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
//        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
//        TakeOperator takeOperator =cloneDag.addOperator(System.currentTimeMillis()+" Take Operator",TakeOperator.class);
//        TakeOperator.count=num;
////        cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
//        cloneDag.addStream(System.currentTimeMillis()+" Take Stream",currentOutputPort,takeOperator.input);
//        runDag(cloneDag,3000);
//        return (T[]) toArray(TakeOperator.elements.iterator());
//    }

    @Override
    public T reduce(Function2<T, T, T> f) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        ReduceOperator reduceOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Reduce " , new ReduceOperator());
       // cloneDag.setInputPortAttribute(reduceOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        reduceOperator.f = context.clean(f,true);
//        reduceOperator.f1=f;
        Assert.assertTrue(currentOutputPort != null);
        cloneDag.addStream(System.currentTimeMillis()+" Reduce Input Stream", currentOutputPort, reduceOperator.input);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, reduceOperator.controlDone);

        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/tmp/outputData");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", reduceOperator.output, writer.input);
        try {
            runDag(cloneDag,3000,"reduce");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return (T) ReduceOperator.finalValue;
    }

    @Override
    public Iterator<T> compute(Partition arg0, TaskContext arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Partition[] getPartitions() {
        // TODO Auto-generated method stub
        ApexPartition[] partitions = new ApexPartition[apexRDDPartitioner.numPartitions()];
        ApexPartition partition = new ApexPartition();
        partitions[0]=partition;
        return partitions;
    }

    @Override
    public long count() {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentCountOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        CountOperator countOperator = cloneDag.addOperator(System.currentTimeMillis()+ " CountOperator " , CountOperator.class);
       // cloneDag.setInputPortAttribute(countOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Count Input Stream", currentCountOutputPort, countOperator.input);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, countOperator.controlDone);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
       // cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/tmp/outputDataCount");
        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", countOperator.output, writer.input);
        try {
            runDag(cloneDag,3000,"count");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Integer count = fileReader("/tmp/outputDataCount");
        if(count==null)
            return 0L;
        return count;
    }

    @Override
    public ApexRDD<T>[] randomSplit(double[] weights, long seed){
        long count =this.count();
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        MyDAG cloneDag2= (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        RandomSplitOperator randomSplitOperator = cloneDag.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        RandomSplitOperator.bitSet=new BitSet((int) count);
        randomSplitOperator.weights=weights;
        randomSplitOperator.count=count;
//        cloneDag.setInputPortAttribute(randomSplitOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentOutputPort, randomSplitOperator.input);
        DefaultOutputPortSerializable currentSplitOutputPort2 = getCurrentOutputPort(cloneDag2);
        RandomSplitOperator randomSplitOperator2 = cloneDag2.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        randomSplitOperator2.weights=weights;
        randomSplitOperator2.flag=true;
        randomSplitOperator2.count=count;
//        cloneDag2.setInputPortAttribute(randomSplitOperator2.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag2.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentSplitOutputPort2, randomSplitOperator2.input);
        ApexRDD<T> temp1= createClone(cloneDag);
        ApexRDD<T> temp2= createClone(cloneDag2);
        ApexRDD[] temp=new ApexRDD[]{temp1, temp2};
        return temp;
    }

    @Override
    public RDD<T> sample(boolean withReplacement, double fraction, long seed) {

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        SampleOperator sampleOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new SampleOperator());
        SampleOperator.fraction = fraction;
//        ScalaApexRDD$.MODULE$.test((ScalaApexRDD<Tuple2<Object, Object>>) this, (ClassTag<Object>) evidence$3,null,null);
        cloneDag.addStream( System.currentTimeMillis()+ " SampleOperatorStream ", currentOutputPort, sampleOperator.input);
        //cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        return createClone(cloneDag);
    }

    @Override
    public <U> U withScope(Function0<U> body) {
        return (U) body;

    }
    public ApexRDD<T> createClone(MyDAG cloneDag){
        ApexRDD<T> apexRDDClone = (ApexRDD<T>) SerializationUtils.clone(this);
        apexRDDClone.dag =cloneDag;
        return apexRDDClone;
    }
    public void runDag(MyDAG cloneDag,long runMillis,String name) throws Exception {
        cloneDag.validate();
        log.debug("DAG successfully validated");
        LocalMode lma = LocalMode.newInstance();
        Configuration conf = new Configuration(true);
//        conf.set("dt.dfsRootDirectory","/user/anurag");
//        conf.set("fs.defaultFS","hdfs://localhost:54310");
//        conf.set("yarn.resourcemanager.address", "localhost:8031");
//        conf.addResource(new File("/home/anurag/datatorrent/current/conf/dt-site.xml").toURI().toURL());
        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        try {
            lma.prepareDAG(app, conf);
        } catch (Exception e) {
            throw new RuntimeException("Exception in prepareDAG", e);
        }
        StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
        appLauncher.loadDependencies();
        StreamingAppFactory appFactory = new StreamingAppFactory(app, name);
        appLauncher.launchApp(appFactory);
//        LocalMode.Controller lc = lma.getController();


    }
    public enum OperatorType {
        INPUT,
        PROCESS,
        OUTPUT
    }
}
