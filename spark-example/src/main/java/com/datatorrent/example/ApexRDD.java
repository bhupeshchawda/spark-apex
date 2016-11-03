package com.datatorrent.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.jblas.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.example.utils.FileWriterOperator;
import com.datatorrent.example.utils.FilterOperator;
import com.datatorrent.example.utils.GenericApplication;
import com.datatorrent.example.utils.JavaSerializationStreamCodec;
import com.datatorrent.example.utils.MapOperator;
import com.datatorrent.example.utils.ReduceOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import junit.framework.Assert;
import scala.Function1;
import scala.Function2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

public class ApexRDD<T> extends RDD<T>
{
  private static final long serialVersionUID = -3545979419189338756L;
  private static Logger LOG = LoggerFactory.getLogger(ApexRDD.class);
  public LogicalPlan dag;
  public DefaultOutputPort<Object> currentOutputPort = null;
  public DefaultOutputPort<Boolean> controlOutputPort = null;
  public ApexRDD(RDD<T> rdd, ClassTag<T> classTag)
  {
    super(rdd, classTag);
  }

  public ApexRDD(ApexContext ac)
  {
    super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
    dag = new LogicalPlan();
  }

  public LogicalPlan getDag()
  {
    return dag;
  }

  @Override
  public <U> RDD<U> map(Function1<T, U> f, ClassTag<U> evidence$3)
  {
    LOG.info("Adding Map Operator");
    MapOperator mapOperator = dag.addOperator("Map"+System.currentTimeMillis(), MapOperator.class);
    dag.setInputPortAttribute(mapOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
    mapOperator.f = f;
    Assert.assertTrue(this.currentOutputPort != null);
    dag.addStream("S_"+System.currentTimeMillis()+Random.nextInt(100), currentOutputPort, mapOperator.input);
    currentOutputPort = mapOperator.output;
    return (ApexRDD<U>)this;
  }

  @Override
  public RDD<T> filter(Function1<T, Object> f)
  {
    LOG.info("Adding Filter Operator");
    FilterOperator filterOperator = dag.addOperator("Filter" + System.currentTimeMillis(), FilterOperator.class);
    dag.setInputPortAttribute(filterOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
    filterOperator.f = f;
    Assert.assertTrue(this.currentOutputPort != null);
    dag.addStream("S_"+System.currentTimeMillis()+Random.nextInt(100), currentOutputPort, filterOperator.input);
    currentOutputPort = filterOperator.output;
    return this;
  }

  @Override
  public T reduce(Function2<T, T, T> f)
  {
    LOG.info("Adding Reduce Operator");
    ReduceOperator reduceOperator = dag.addOperator("Reduce" + System.currentTimeMillis(), ReduceOperator.class);
    dag.setInputPortAttribute(reduceOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
    reduceOperator.f = f;

    Assert.assertTrue(this.currentOutputPort != null);
    dag.addStream("S_"+System.currentTimeMillis()+Random.nextInt(100), currentOutputPort, reduceOperator.input);
    dag.addStream("S_"+System.currentTimeMillis()+Random.nextInt(100), controlOutputPort, reduceOperator.controlDone);

    LOG.info("Adding Writer Operator");
    FileWriterOperator writer = dag.addOperator("Writer" + System.currentTimeMillis(), FileWriterOperator.class);
    dag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
    writer.setAbsoluteFilePath("/tmp/outputData");
    dag.addStream("S_"+System.currentTimeMillis()+Random.nextInt(100), reduceOperator.output, writer.input);

    System.out.println(dag);
    dag.validate();
    System.out.println("DAG successfully validated");

    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
//    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    GenericApplication app = new GenericApplication();
    app.setDag(dag);
    try {
      lma.prepareDAG(app, conf);
    } catch (Exception e) {
      throw new RuntimeException("Exception in prepareDAG", e);
    }
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);

    return (T) (new Integer(0));
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
