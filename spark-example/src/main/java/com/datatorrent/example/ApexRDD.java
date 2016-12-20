package com.datatorrent.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.LocalMode;
import com.datatorrent.example.apexscala.ScalaApexRDD;
import com.datatorrent.example.utils.*;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.RDDOperationScope$;
import org.apache.spark.rdd.ZippedPartitionsRDD2;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.*;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.lang.Boolean;
import java.util.Random;
@DefaultSerializer(JavaSerializer.class)
public class ApexRDD<T> extends ScalaApexRDD<T> implements Serializable {
    private static final long serialVersionUID = -3545979419189338756L;
    private static PairRDDFunctions temp;
    public static ApexContext context;
    public MyBaseOperator currentOperator;
    public OperatorType currentOperatorType;
    public DefaultOutputPortSerializable currentOutputPort;
    public DefaultOutputPortSerializable<Boolean> controlOutput;
    public  MyDAG dag;

    public ApexRDDPartitioner apexRDDPartitioner = new ApexRDDPartitioner();
    protected Option<Partitioner> partitioner = new ApexRDDOptionPartitioner();


    public ApexRDD(RDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
        this.dag=((ApexRDD<T>)rdd).dag;

    }
    @Override
    public Option<Partitioner> partitioner() {
        return  new ApexRDDOptionPartitioner();
    }

    @Override
    public SparkContext context() {
        return context;
    }

    public ApexRDD(ApexContext ac) {
        super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
        dag = new MyDAG();
        context=ac;
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
    public <U> RDD<U> map(Function <T,U> f){

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapFunctionOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new MapFunctionOperator());
        m1.f=f;

//        ScalaApexRDD$.MODULE$.test((ScalaApexRDD<Tuple2<Object, Object>>) this, (ClassTag<Object>) evidence$3,null,null);
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream ", currentOutputPort, m1.input);
        cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<U> temp= (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag=cloneDag;
        return temp;
    }

    @Override
    public <B, V> RDD<V> zipPartitions(RDD<B> rdd2, Function2<Iterator<T>, Iterator<B>,
            Iterator<V>> f, ClassTag<B> evidence$12, ClassTag<V> evidence$13) {
        Assert.assertTrue(false);
        ApexRDD<V> temp = (ApexRDD<V>) super.zipPartitions(rdd2, f, evidence$12, evidence$13);
        temp.dag= (MyDAG) SerializationUtils.clone(this.dag);

        return temp;
    }

    @Override
    public <B, V> RDD<V> zipPartitions(RDD<B> rdd2, boolean preservesPartitioning, Function2<Iterator<T>, Iterator<B>, Iterator<V>> f, ClassTag<B> evidence$10, ClassTag<V> evidence$11) {
        ApexRDD<V> temp = (ApexRDD<V>) super.zipPartitions(rdd2, f, evidence$10, evidence$11);
        temp.dag= (MyDAG) SerializationUtils.clone(this.dag);
        return  temp;

    }

    @Override
    public <B, C, V> RDD<V> zipPartitions(RDD<B> rdd2, RDD<C> rdd3, boolean preservesPartitioning, Function3<Iterator<T>, Iterator<B>, Iterator<C>, Iterator<V>> f, ClassTag<B> evidence$14, ClassTag<C> evidence$15, ClassTag<V> evidence$16) {
        Assert.assertTrue("2",false);
        return super.zipPartitions(rdd2, rdd3, preservesPartitioning, f, evidence$14, evidence$15, evidence$16);
    }

    @Override
    public <B, C, V> RDD<V> zipPartitions(RDD<B> rdd2, RDD<C> rdd3, Function3<Iterator<T>, Iterator<B>, Iterator<C>, Iterator<V>> f, ClassTag<B> evidence$17, ClassTag<C> evidence$18, ClassTag<V> evidence$19) {
        Assert.assertTrue("3",false);
        return super.zipPartitions(rdd2, rdd3, f, evidence$17, evidence$18, evidence$19);
    }

    @Override
    public <B, C, D, V> RDD<V> zipPartitions(RDD<B> rdd2, RDD<C> rdd3, RDD<D> rdd4, boolean preservesPartitioning, Function4<Iterator<T>, Iterator<B>, Iterator<C>, Iterator<D>, Iterator<V>> f, ClassTag<B> evidence$20, ClassTag<C> evidence$21, ClassTag<D> evidence$22, ClassTag<V> evidence$23) {
        Assert.assertTrue("4",false);
        return super.zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning, f, evidence$20, evidence$21, evidence$22, evidence$23);
    }

    @Override
    public <B, C, D, V> RDD<V> zipPartitions(RDD<B> rdd2, RDD<C> rdd3, RDD<D> rdd4, Function4<Iterator<T>, Iterator<B>, Iterator<C>, Iterator<D>, Iterator<V>> f, ClassTag<B> evidence$24, ClassTag<C> evidence$25, ClassTag<D> evidence$26, ClassTag<V> evidence$27) {
        Assert.assertTrue("5",false);
        return super.zipPartitions(rdd2, rdd3, rdd4, f, evidence$24, evidence$25, evidence$26, evidence$27);
    }

    @Override
    public <U> U withScope(Function0<U> body) {
        return RDDOperationScope$.MODULE$.withScope(context,false,body);
    }


    @Override
    public <U> RDD<U> map(Function1<T, U> f, ClassTag<U> evidence$3) {

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new MapOperator());
        m1.f=f;

//        ScalaApexRDD$.MODULE$.test((ScalaApexRDD<Tuple2<Object, Object>>) this, (ClassTag<Object>) evidence$3,null,null);
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream ", currentOutputPort, m1.input);
        cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<U> temp= (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag=cloneDag;
        return temp;
    }

    @Override
    public RDD<T> filter(Function1<T, Object> f) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        FilterOperator filterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Filter", FilterOperator.class);
        filterOperator.f = f;
        cloneDag.addStream(System.currentTimeMillis()+ " FilterStream " + 1, currentOutputPort, filterOperator.input);
        ApexRDD<T> temp= (ApexRDD<T>) SerializationUtils.clone(this);
        temp.dag=cloneDag;
        return temp;
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
        lc.run(3000);
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
        ApexPartition[] partitions = new ApexPartition[apexRDDPartitioner.numPartitions()];
        ApexPartition apexPartition= new ApexPartition();
        partitions[0]=apexPartition;
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
        lc.run(3000);

        Integer count = fileReader("/tmp/outputDataCount");
        return count;
    }

    @Override
    public ApexRDD<T>[] randomSplit(double[] weights, long seed){
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        MyDAG cloneDag2= (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);

        RandomSplitOperator randomSplitOperator = cloneDag.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        randomSplitOperator.weights=weights;
//        cloneDag.setInputPortAttribute(randomSplitOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentOutputPort, randomSplitOperator.input);
        DefaultOutputPortSerializable currentSplitOutputPort2 = getCurrentOutputPort(cloneDag2);
        RandomSplitOperator randomSplitOperator2 = cloneDag2.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        randomSplitOperator2.weights=weights;
        randomSplitOperator2.flag=true;
//        cloneDag2.setInputPortAttribute(randomSplitOperator2.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag2.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentSplitOutputPort2, randomSplitOperator2.input);
        ApexRDD<T> temp1= (ApexRDD<T>) SerializationUtils.clone(this);
        temp1.dag=cloneDag;
        ApexRDD<T> temp2= (ApexRDD<T>) SerializationUtils.clone(this);
        temp2.dag=cloneDag2;
        ApexRDD[] temp=new ApexRDD[]{temp1, temp2};
        return temp;
    }

    @Override
    public T[] collect() {
        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        CollectOperator collectOperator =cloneDag.addOperator(System.currentTimeMillis()+" Collect Operator",CollectOperator.class);
        ApexRDDStreamCodec a = new ApexRDDStreamCodec();
        cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, a);
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
        lc.run(3000);
        T[] array= (T[]) CollectOperator.t.toArray();

        return array;
    }



    public class PairApexRDDFunctions<K,V> extends ApexRDD {
        public PairApexRDDFunctions(RDD<T> rdd, ClassTag<T> classTag) {
            super(rdd, classTag);
        }
        public <C> ApexRDD<Tuple2<K, C>> combineByKey(Function1<V, C> createCombiner, Function2<C, V, C> mergeValue,
                                                  Function2<C, C, C> mergeCombiners, Partitioner partitioner,
                                                  boolean mapSideCombine, Serializer serializer) {
//            ApexRDD<Tuple2<K,C>> temp =(ApexRDD<Tuple2<K,C>> )super.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer);

            log.info("combineByKey is called");
//            junit.framework.Assert.assertTrue(false);
            return null;
        }

        public <C> ApexRDD<Tuple2<K, C>> combineByKey(Function1<V, C> createCombiner,
                                                  Function2<C, V, C> mergeValue, Function2<C, C, C> mergeCombiners,
                                                  int numPartitions) {
            log.info("combineByKey is called +1");
//            ApexRDD<Tuple2<K,C>>  temp=( ApexRDD<Tuple2<K,C>> ) super.combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions);
            return null;
        }
    }

    public enum OperatorType {
        INPUT,
        PROCESS,
        OUTPUT
    }
}
