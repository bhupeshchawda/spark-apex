package com.datatorrent.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.LocalMode;
import com.datatorrent.example.apexscala.ScalaApexRDD;
import com.datatorrent.example.utils.*;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.math.random.RandomGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.sql.catalyst.expressions.AssertTrue;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.*;
import scala.collection.Iterator;
import scala.collection.TraversableOnce;
import scala.reflect.ClassTag;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.lang.Boolean;
import java.util.*;

import static java.util.Arrays.*;


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
    public SparkContext sparkContext() {
        return context;
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
    public <U> RDD<U> flatMap(Function1<T, TraversableOnce<U>> f, ClassTag<U> evidence$4) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Flat Map " , new MapOperator());
        m1.f=f;
        cloneDag.addStream( System.currentTimeMillis()+ " FlatMapStream ", currentOutputPort, m1.input);
        cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<U> temp= (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag=cloneDag;
        return temp;
    }

    @Override
    public <U> RDD<Tuple2<T, U>> zip(RDD<U> other, ClassTag<U> evidence$9) {
        other.collect();
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        ZipOperator<T,U> z = cloneDag.addOperator(System.currentTimeMillis()+ " ZipOperator " , new ZipOperator<T,U>());
        z.other= Arrays.asList((T[]) other.collect());
        z.count=z.other.size();
        cloneDag.addStream( System.currentTimeMillis()+ " ZipOperator ", currentOutputPort, z.input);
        cloneDag.setInputPortAttribute(z.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<Tuple2<T,U>> temp= (ApexRDD<Tuple2<T,U>>) SerializationUtils.clone(this);
        temp.dag=cloneDag;
        return temp;
    }

    @Override
    public T[] takeSample(boolean withReplacement, int num, long seed) {
        Random random = new Random(seed);
        T[] temp1 = this.collect();

        ArrayList<T> temp2 = new ArrayList<>();
        for(int i=0;i<num;i++){
            temp2.add(temp1[random.nextInt(temp1.length)]);
        }

        return (T[]) temp2.toArray();
    }

    @Override
    public <U> U aggregate(U zeroValue, Function2<U, T, U> seqOp, Function2<U, U, U> combOp, ClassTag<U> evidence$29) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        AggregateOperator aggregateOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Aggregate Operator " , new AggregateOperator());
        aggregateOperator.zeroValue = zeroValue;
        aggregateOperator.seqFunction = seqOp;
        aggregateOperator.combFunction = combOp;
        Assert.assertTrue(currentOutputPort != null);
        cloneDag.addStream(System.currentTimeMillis()+" Aggregator Operator Stream", currentOutputPort, aggregateOperator.input);
        FileWriterOperator fileWriterOperator = cloneDag.addOperator(System.currentTimeMillis()+"FileWrite Operator from Aggregate",new FileWriterOperator());
        fileWriterOperator.setAbsoluteFilePath("/tmp/aggregateOutput");
        cloneDag.addStream(System.currentTimeMillis()+"File Operator Stream",aggregateOperator.output,fileWriterOperator.input);
        cloneDag.validate();
        log.debug("DAG successfully validated");

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

        return (U) aggregateOperator.aggregateValue;
    }

    @Override
    public <U> U withScope(Function0<U> body) {
        return (U)body;
    }

    @Override
    public RDD<T> unpersist(boolean blocking) {
        return this;
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
    public <U> RDD<U> mapPartitionsWithIndex(Function2<Object, Iterator<T>, Iterator<U>> f, boolean preservesPartitioning, ClassTag<U> evidence$8) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapPartitionsWithIndexOperator m = cloneDag.addOperator(System.currentTimeMillis()+ " MapPartitionsWithIndex " , new MapPartitionsWithIndexOperator());
        m.f = f;
        m.preservePartitioning = preservesPartitioning;
        cloneDag.addStream(System.currentTimeMillis()+" MapPartitionsWithIndex Input Stream",currentOutputPort,m.input);
        ApexRDD<U> temp = (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag = cloneDag;
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
        log.debug("DAG successfully validated");

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

        log.debug("DAG successfully validated");
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
//        ApexRDDStreamCodec a = new ApexRDDStreamCodec();
//        cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, a);
        cloneDag.addStream(System.currentTimeMillis()+" Collect Stream",currentOutputPort,collectOperator.input);
        cloneDag.validate();

        log.debug("DAG successfully validated");
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

            log.debug("combineByKey is called");
//            junit.framework.Assert.assertTrue(false);
            return null;
        }

        public <C> ApexRDD<Tuple2<K, C>> combineByKey(Function1<V, C> createCombiner,
                                                  Function2<C, V, C> mergeValue, Function2<C, C, C> mergeCombiners,
                                                  int numPartitions) {
            log.debug("combineByKey is called +1");
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
