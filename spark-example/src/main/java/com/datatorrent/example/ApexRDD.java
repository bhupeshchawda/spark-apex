package com.datatorrent.example;

import com.datatorrent.api.LocalMode;
import com.datatorrent.example.apexscala.ApexPartition;
import com.datatorrent.example.apexscala.ScalaApexRDD;
import com.datatorrent.example.utils.*;
import com.datatorrent.stram.client.StramAppLauncher;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
    public Object readFromFile(String path) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(path);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object result = ois.readObject();
        ois.close();
        return result;

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
    public DefaultInputPortSerializable getFirstInputPort(MyDAG cloneDag){
        BaseInputOperator currentOperator= (BaseInputOperator) cloneDag.getOperatorMeta(cloneDag.getFirstOperatorName()).getOperator();
        return currentOperator.getInputPort();
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
        return this.take(1)[0];
    }

    @Override
    public T[] collect() {
        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        CollectOperator collectOperator =cloneDag.addOperator(System.currentTimeMillis()+" Collect Operator",CollectOperator.class);

//        cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Collect Stream",currentOutputPort,collectOperator.input);
        try {
            runDag(cloneDag,3000," collect");
        } catch (Exception e) {
            e.printStackTrace();
        }
        T[] array= (T[]) CollectOperator.dataList.toArray();
        return array;
    }

    @Override
    public T[] take(int num) {
//        ArrayList<T> a = new ArrayList<>(num);
//        Object[] ret = this.collect();
//        for ( int i=0; i< num; i++) {
//            a.add((T) ret[i]);
//        }
//    return a.toArray((T[]) Array.newInstance(this.getClass().getTypeParameters()[0].getClass(), num));
        if(num==1){

        }
        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        TakeOperator takeOperator =cloneDag.addOperator(System.currentTimeMillis()+" Take Operator",TakeOperator.class);
        takeOperator.count=num;
//        cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Collect Stream",currentOutputPort,takeOperator.input);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/tmp/spark-apex/selectedData.ser");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", takeOperator.output, writer.input);
        try {
            runDag(cloneDag,3000,"take");
        } catch (Exception e) {
            e.printStackTrace();
        }
        T[] array= null;
        try {
            array = (T[]) readFromFile("/tmp/spark-apex/selectedData.ser");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return array;
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
        DefaultInputPortSerializable firstInputPort = getFirstInputPort(cloneDag);
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
        writer.setAbsoluteFilePath("/tmp/spark-apex/outputData");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", reduceOperator.output, writer.input);

        try {
            runDag(cloneDag,3000,"reduce");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            T reducedValue= (T) readFromFile("/tmp/spark-apex/outputData");
            return reducedValue;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }


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
        writer.setAbsoluteFilePath("/tmp/spark-apex/outputDataCount");
        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", countOperator.output, writer.input);
        try {
            runDag(cloneDag,3000,"count");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Long count = 0L;
        try {
            count = (Long) readFromFile("/tmp/spark-apex/outputDataCount");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
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
        String jars="/home/anurag/spark-apex/spark-example/OUTPUT_DIR/activation-1.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/activemq-client-5.8.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/ant-1.9.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/ant-launcher-1.9.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/antlr4-runtime-4.5.3.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/aopalliance-1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/aopalliance-repackaged-2.4.0-b34.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/apex-api-3.5.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/apex-bufferserver-3.5.0-SNAPSHOT.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/apex-common-3.4.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/apex-engine-3.5.0-SNAPSHOT.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/apex-shaded-ning19-1.0.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/arpack_combined_all-0.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/asm-3.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/avro-1.7.4.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/avro-ipc-1.7.7.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/avro-ipc-1.7.7-tests.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/avro-mapred-1.7.7-hadoop2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/aws-java-sdk-core-1.10.73.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/aws-java-sdk-kms-1.10.73.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/aws-java-sdk-s3-1.10.73.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/breeze_2.10-0.12.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/breeze-macros_2.10-0.12.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/bval-core-0.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/bval-jsr303-0.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/chill_2.10-0.8.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/chill-java-0.8.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-beanutils-1.8.3.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-cli-1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-codec-1.10.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-collections-3.2.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-compiler-2.7.8.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-compress-1.4.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-configuration-1.6.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-crypto-1.0.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-digester-1.8.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-el-1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-httpclient-3.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-io-2.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-lang-2.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-lang3-3.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-logging-1.1.3.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-math-2.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-math3-3.4.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/commons-net-2.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/compress-lzf-1.0.3.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/core-1.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/curator-client-2.4.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/curator-framework-2.4.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/curator-recipes-2.4.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/fastutil-7.0.6.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/geronimo-j2ee-management_1.1_spec-1.0.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/geronimo-jms_1.1_spec-1.1.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/gmbal-api-only-3.0.0-b023.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/grizzly-framework-2.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/grizzly-http-2.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/grizzly-http-server-2.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/grizzly-http-servlet-2.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/grizzly-rcm-2.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/guava-14.0.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/guice-3.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/guice-servlet-3.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-annotations-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-auth-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-client-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-common-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-common-2.2.0-tests.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-hdfs-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-app-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-common-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-core-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-jobclient-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-shuffle-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-yarn-api-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-yarn-client-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-yarn-common-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hadoop-yarn-server-common-2.2.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hawtbuf-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hk2-api-2.4.0-b34.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hk2-locator-2.4.0-b34.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/hk2-utils-2.4.0-b34.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/httpclient-4.3.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/httpcore-4.3.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/ivy-2.4.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jackson-annotations-2.6.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jackson-core-2.6.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jackson-core-asl-1.9.13.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jackson-databind-2.6.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jackson-dataformat-cbor-2.5.3.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jackson-mapper-asl-1.9.13.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jackson-module-paranamer-2.6.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jackson-module-scala_2.10-2.6.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/janino-3.0.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jars,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jasper-compiler-5.5.23.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jasper-runtime-5.5.23.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/javassist-3.18.1-GA.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/javax.annotation-api-1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/javax.inject-1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/javax.inject-2.4.0-b34.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/javax.mail-1.5.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/javax.servlet-3.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/javax.servlet-api-3.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/javax.ws.rs-api-2.0.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jaxb-api-2.2.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jaxb-impl-2.2.3-1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jcl-over-slf4j-1.7.16.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jctools-core-1.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-apache-client4-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-client-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-client-2.22.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-common-2.22.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-container-servlet-2.22.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-container-servlet-core-2.22.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-core-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-grizzly2-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-guava-2.22.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-guice-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-json-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-media-jaxb-2.22.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-server-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-server-2.22.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-test-framework-core-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jersey-test-framework-grizzly2-1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jets3t-0.7.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jettison-1.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-6.1.26.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-continuation-8.1.10.v20130312.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-http-8.1.10.v20130312.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-io-8.1.10.v20130312.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-security-8.1.10.v20130312.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-server-8.1.10.v20130312.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-servlet-8.1.10.v20130312.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-util-6.1.26.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-util-8.1.10.v20130312.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jetty-websocket-8.1.10.v20130312.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jline-2.11.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jms-api-1.1-rev-1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/joda-time-2.9.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jooq-3.6.4.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jsch-0.1.42.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/json4s-ast_2.10-3.2.11.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/json4s-core_2.10-3.2.11.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/json4s-jackson_2.10-3.2.11.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jsp-api-2.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jsr305-1.3.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jtransforms-2.4.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/jul-to-slf4j-1.7.16.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/junit-4.8.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/kryo-2.24.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/kryo-shaded-3.0.3.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/leveldbjni-all-1.8.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/log4j-1.2.17.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/lz4-1.3.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/malhar-library-3.5.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/management-api-3.0.0-b012.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/mbassador-1.1.9.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/metrics-core-3.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/metrics-graphite-3.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/metrics-json-3.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/metrics-jvm-3.1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/minlog-1.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/minlog-1.3.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/named-regexp-0.2.3.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/netlet-1.3.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/netty-3.8.0.Final.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/netty-all-4.0.42.Final.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/objenesis-2.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/opencsv-2.3.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/oro-2.0.8.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/osgi-resource-locator-1.0.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/paranamer-2.6.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/parquet-column-1.8.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/parquet-common-1.8.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/parquet-encoding-1.8.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/parquet-format-2.3.0-incubating.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/parquet-hadoop-1.8.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/parquet-jackson-1.8.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/pmml-model-1.2.15.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/pmml-schema-1.2.15.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/protobuf-java-2.5.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/py4j-0.10.4.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/pyrolite-4.13.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/quasiquotes_2.10-2.0.0-M8.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/RoaringBitmap-0.5.11.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/scala-compiler-2.10.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/scala-library-2.10.6.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/scalap-2.10.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/scala-reflect-2.10.6.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/scalatest_2.10-2.2.6.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/servlet-api-2.5.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/shapeless_2.10.4-2.0.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/slf4j-api-1.7.16.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/slf4j-log4j12-1.7.16.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/snappy-java-1.1.2.6.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-catalyst_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-core_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-graphx_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-launcher_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-mllib_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-mllib-local_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-network-common_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-network-shuffle_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-sketch_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-sql_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-streaming_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-tags_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spark-unsafe_2.10-2.1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spire_2.10-0.7.4.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/spire-macros_2.10-0.7.4.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/stax-api-1.0.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/stream-2.7.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/univocity-parsers-2.2.1.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/unused-1.0.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/validation-api-1.1.0.Final.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/xbean-asm5-shaded-4.4.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/xmlenc-0.52.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/xz-1.0.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/zip4j-1.3.2.jar,/home/anurag/spark-apex/spark-example/OUTPUT_DIR/zookeeper-3.4.5.jar";
        log.debug("DAG successfully validated");
//        LocalMode lma = LocalMode.newInstance();
        Configuration conf = new Configuration(true);
//        conf.set("dt.dfsRootDirectory","/user/anurag");
        conf.set("fs.defaultFS","hdfs://localhost:54310");
        conf.set("yarn.resourcemanager.address", "localhost:8032");
        conf.addResource(new File("/home/anurag/spark-apex/spark-example/src/main/resources/properties.xml").toURI().toURL());
        conf.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME,jars);
//        conf.addResource(new File("/home/anurag/datatorrent/current/conf/dt-site.xml").toURI().toURL());
        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
//        try {
//            lma.prepareDAG(app, conf);
//        } catch (Exception e) {
//            throw new RuntimeException("Exception in prepareDAG", e);
//        }
//        YarnAppLauncher launcher = Launcher.getLauncher(Launcher.LaunchMode.YARN);
//        launcher.launchApp(app,conf);
        File successFile = new File("/tmp/spark-apex/_SUCCESS");
        if(successFile.exists())    {
            successFile.delete();
        }
        StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
        appLauncher.loadDependencies();
        StreamingAppFactory appFactory = new StreamingAppFactory(app, name);
        ApplicationId id = appLauncher.launchApp(appFactory);
        YarnConfiguration conf2 = new YarnConfiguration();
        conf2.addResource("/home/anurag/hadoop/hadoop-2.7.2/etc/hadoop/yarn-site.xml");
        YarnClient client = YarnClient.createYarnClient();

        client.init(conf2);
        client.start();
        while(!successFile.exists()) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.killApplication(id);
        client.stop();
        log.info("Killed Application {} {}",name,id);


    }
    public void runDagLocal(MyDAG cloneDag,long runMillis,String name) {
        cloneDag.validate();
        log.info("DAG successfully validated {}",name);
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
        File successFile = new File("/tmp/spark-apex/_SUCCESS");
        if(successFile.exists())    successFile.delete();
        lc.runAsync();
        while(!successFile.exists()) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lc.shutdown();


    }
    public enum OperatorType {
        INPUT,
        PROCESS,
        OUTPUT
    }
}
