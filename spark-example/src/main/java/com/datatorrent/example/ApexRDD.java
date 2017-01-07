package com.datatorrent.example;

import com.datatorrent.api.LocalMode;
import com.datatorrent.example.scala.ApexPartition;
import com.datatorrent.example.scala.ApexRDDs;
import com.datatorrent.example.utils.*;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LabeledPoint$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.RDDOperationScope$;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.Function2;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Map;
import scala.math.Ordering;
import scala.reflect.ClassTag;

import java.io.*;
import java.util.HashMap;
import java.util.Random;

@DefaultSerializer(JavaSerializer.class)
public class ApexRDD<T> extends ApexRDDs<T> implements java.io.Serializable {
    private static final long serialVersionUID = -3545979419189338756L;
    private static final String RDDLIBJARS= "/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/activation-1.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/activemq-client-5.8.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/ant-1.9.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/ant-launcher-1.9.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/antlr4-runtime-4.5.3.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/aopalliance-1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/aopalliance-repackaged-2.4.0-b34.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/apex-api-3.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/apex-bufferserver-3.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/apex-common-3.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/apex-engine-3.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/apex-shaded-ning19-1.0.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/arpack_combined_all-0.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/asm-3.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/avro-1.7.4.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/avro-ipc-1.7.7.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/avro-ipc-1.7.7-tests.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/avro-mapred-1.7.7-hadoop2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/aws-java-sdk-core-1.10.73.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/aws-java-sdk-kms-1.10.73.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/aws-java-sdk-s3-1.10.73.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/breeze_2.10-0.12.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/breeze-macros_2.10-0.12.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/bval-core-0.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/bval-jsr303-0.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/chill_2.10-0.8.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/chill-java-0.8.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-beanutils-1.8.3.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-cli-1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-codec-1.10.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-collections-3.2.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-compiler-2.7.8.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-compress-1.4.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-configuration-1.6.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-crypto-1.0.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-digester-1.8.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-el-1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-httpclient-3.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-io-2.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-lang-2.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-lang3-3.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-logging-1.1.3.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-math-2.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-math3-3.4.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/commons-net-2.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/compress-lzf-1.0.3.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/core-1.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/curator-client-2.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/curator-framework-2.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/curator-recipes-2.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/fastutil-7.0.6.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/geronimo-j2ee-management_1.1_spec-1.0.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/geronimo-jms_1.1_spec-1.1.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/gmbal-api-only-3.0.0-b023.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/grizzly-framework-2.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/grizzly-http-2.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/grizzly-http-server-2.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/grizzly-http-servlet-2.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/grizzly-rcm-2.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/guava-14.0.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/guice-3.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/guice-servlet-3.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-annotations-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-auth-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-client-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-common-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-common-2.2.0-tests.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-hdfs-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-app-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-common-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-core-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-jobclient-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-mapreduce-client-shuffle-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-yarn-api-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-yarn-client-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-yarn-common-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hadoop-yarn-server-common-2.2.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hawtbuf-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hk2-api-2.4.0-b34.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hk2-locator-2.4.0-b34.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/hk2-utils-2.4.0-b34.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/httpclient-4.3.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/httpcore-4.3.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/ivy-2.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jackson-annotations-2.6.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jackson-core-2.6.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jackson-core-asl-1.9.13.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jackson-databind-2.6.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jackson-dataformat-cbor-2.5.3.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jackson-mapper-asl-1.9.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jackson-module-paranamer-2.6.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jackson-module-scala_2.10-2.6.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/janino-3.0.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jasper-compiler-5.5.23.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jasper-runtime-5.5.23.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/javassist-3.18.1-GA.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/javax.annotation-api-1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/javax.inject-1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/javax.inject-2.4.0-b34.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/javax.mail-1.5.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/javax.servlet-3.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/javax.servlet-api-3.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/javax.ws.rs-api-2.0.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jaxb-api-2.2.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jaxb-impl-2.2.3-1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jcl-over-slf4j-1.7.16.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jctools-core-1.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-apache-client4-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-client-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-client-2.22.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-common-2.22.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-container-servlet-2.22.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-container-servlet-core-2.22.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-core-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-grizzly2-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-guava-2.22.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-guice-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-json-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-media-jaxb-2.22.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-server-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-server-2.22.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-test-framework-core-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jersey-test-framework-grizzly2-1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jets3t-0.7.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jettison-1.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-6.1.26.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-continuation-8.1.10.v20130312.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-http-8.1.10.v20130312.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-io-8.1.10.v20130312.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-security-8.1.10.v20130312.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-server-8.1.10.v20130312.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-servlet-8.1.10.v20130312.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-util-6.1.26.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-util-8.1.10.v20130312.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jetty-websocket-8.1.10.v20130312.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jline-2.11.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jms-api-1.1-rev-1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/joda-time-2.9.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jooq-3.6.4.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jsch-0.1.42.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/json4s-ast_2.10-3.2.11.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/json4s-core_2.10-3.2.11.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/json4s-jackson_2.10-3.2.11.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jsp-api-2.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jsr305-1.3.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jtransforms-2.4.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/jul-to-slf4j-1.7.16.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/junit-4.8.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/kryo-2.24.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/kryo-shaded-3.0.3.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/leveldbjni-all-1.8.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/log4j-1.2.17.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/lz4-1.3.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/malhar-library-3.5.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/management-api-3.0.0-b012.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/mbassador-1.1.9.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/metrics-core-3.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/metrics-graphite-3.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/metrics-json-3.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/metrics-jvm-3.1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/minlog-1.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/minlog-1.3.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/named-regexp-0.2.3.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/netlet-1.2.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/netty-3.8.0.Final.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/netty-all-4.0.42.Final.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/objenesis-2.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/opencsv-2.3.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/oro-2.0.8.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/osgi-resource-locator-1.0.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/paranamer-2.6.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/parquet-column-1.8.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/parquet-common-1.8.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/parquet-encoding-1.8.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/parquet-format-2.3.0-incubating.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/parquet-hadoop-1.8.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/parquet-jackson-1.8.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/pmml-model-1.2.15.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/pmml-schema-1.2.15.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/protobuf-java-2.5.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/py4j-0.10.4.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/pyrolite-4.13.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/quasiquotes_2.10-2.0.0-M8.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/RoaringBitmap-0.5.11.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/scala-compiler-2.10.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/scala-library-2.10.6.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/scalap-2.10.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/scala-reflect-2.10.6.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/scalatest_2.10-2.2.6.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/servlet-api-2.5.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/shapeless_2.10.4-2.0.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/slf4j-api-1.7.16.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/slf4j-log4j12-1.7.16.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/snappy-java-1.1.2.6.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-catalyst_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-core_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-graphx_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-launcher_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-mllib_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-mllib-local_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-network-common_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-network-shuffle_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-sketch_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-sql_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-streaming_2.10-2.0.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-tags_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spark-unsafe_2.10-2.1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spire_2.10-0.7.4.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/spire-macros_2.10-0.7.4.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/stax-api-1.0.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/stream-2.7.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/univocity-parsers-2.2.1.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/unused-1.0.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/validation-api-1.1.0.Final.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/xbean-asm5-shaded-4.4.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/xmlenc-0.52.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/xz-1.0.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/zip4j-1.3.2.jar,/home/harsh/apex-integration/spark-apex/spark-example/OUTPUT_DIR/zookeeper-3.4.5.jar";
    public MyBaseOperator currentOperator;
    public OperatorType currentOperatorType;
    public DefaultOutputPortSerializable currentOutputPort;
    public DefaultOutputPortSerializable<Boolean> controlOutput;
    public  MyDAG dag;
    public static final String reduceApp = "Reduce";
    public static final String firstApp = "First";
    public static final String countByValueApp = "CountByValue";
    public ApexRDDPartitioner apexRDDPartitioner = new ApexRDDPartitioner();
    protected Option partitioner = new ApexRDDOptionPartitioner();
    public static ApexContext context;



    public ApexRDD(ApexContext ac) {
        super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
        context= ac;
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

    @Override
    public SparkContext sparkContext() {
        return context;
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
        //cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<U> temp = (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag = cloneDag;
        return temp;
    }

    public <U> RDD<U> map(Function<T, T> f) {

        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapFunctionOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map1 " , new MapFunctionOperator());
        m1.ff=f;
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream1 ", currentOutputPort, m1.input);
        //cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
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
        ApexRDD<T> temp = (ApexRDD<T>) SerializationUtils.clone(this);
        temp.dag = (MyDAG) SerializationUtils.clone(cloneDag);
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
        //cloneDag.setInputPortAttribute(reduceOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        reduceOperator.f = context.clean(f,true);

        Assert.assertTrue(currentOutputPort != null);
        cloneDag.addStream(System.currentTimeMillis()+" Reduce Input Stream", currentOutputPort, reduceOperator.input);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, reduceOperator.controlDone);

        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/harsh/chi/outputData");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", reduceOperator.output, writer.input);

        cloneDag.validate();
        log.info("DAG successfully validated");

        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        ApexDoTask apexDoTask = new ApexDoTask();
        try {
            apexDoTask.launch(app,reduceApp,RDDLIBJARS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Integer reduce = fileReader("hdfs://localhost:54310/harsh/chi/outputData");
        return (T) reduce;
    }

    public static Integer fileReader(String path){
        BufferedReader br = null;
        FileSystem hdfs=null;
        try{
            Configuration conf = new Configuration();
            Path pt=new Path(path);
            hdfs = FileSystem.get(pt.toUri(), conf);
            br=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
            String line = br.readLine();
            while (line!=null)
                return Integer.valueOf(line);
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            try{
                if(br!=null)
                    br.close();
                    hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    public static String SfileReader(String path){
        BufferedReader br = null;
        FileSystem hdfs=null;
        try{
            Configuration conf = new Configuration();
            Path pt=new Path(path);
            hdfs = FileSystem.get(pt.toUri(), conf);
            br=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
            String line = br.readLine();
            while (line!=null)
                return line;
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            try{
                if(br!=null)
                    br.close();
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    public static HashMap ObjectReader(String path){
        BufferedReader br = null;
        FileSystem hdfs=null;
        File toRead=new File(path);
        FileInputStream fis= null;
        HashMap mapInFile = null;
        try{
            Configuration conf = new Configuration();
            Path pt=new Path(path);
            hdfs = FileSystem.get(pt.toUri(), conf);
            ObjectInputStream ois=new ObjectInputStream((hdfs.open(pt)));
            mapInFile=(HashMap)ois.readObject();
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            try {
                if (br != null)
                    br.close();
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return mapInFile;
    }

    @Override
    public Iterator<T> compute(Partition arg0, TaskContext arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Partition[] getPartitions() {
        // TODO Auto-generated method stub
        ApexPartition apexPartition = new ApexPartition();
        Partition[] partitions = new Partition[apexRDDPartitioner.numPartitions()];
        partitions[0]=apexPartition;
        return partitions;
    }
    @Override
    public long count() {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);

        DefaultOutputPortSerializable currentCountOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        CountOperator countOperator = cloneDag.addOperator(System.currentTimeMillis()+ " CountOperator " , CountOperator.class);
        //cloneDag.setInputPortAttribute(countOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Count Input Stream", currentCountOutputPort, countOperator.input);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, countOperator.controlDone);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/harsh/chi/outputDataCount");
        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", countOperator.output, writer.input);
        cloneDag.validate();

        log.info("DAG successfully validated");
        LocalMode lma = LocalMode.newInstance();
        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        ApexDoTask apexDoTask = new ApexDoTask();
        try {
            apexDoTask.launch(app,"ChiCount",RDDLIBJARS);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Integer count = fileReader("/harsh/chi/outputDataCount");
        return count;
    }

    @Override
    public T first() {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        FirstOpertaor firstOpertaor = cloneDag.addOperator(System.currentTimeMillis()+" FirstOperator",FirstOpertaor.class);
        //cloneDag.setInputPortAttribute(firstOpertaor.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+"FirstTupleStream",currentOutputPort,firstOpertaor.input);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/harsh/chi/outputDataFirst");
        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", firstOpertaor.output, writer.input);
        cloneDag.validate();
        log.info("DAG successfully validated");

        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        ApexDoTask apexDoTask = new ApexDoTask();
        try {
            apexDoTask.launch(app,firstApp,RDDLIBJARS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String first = SfileReader("hdfs://localhost:54310/harsh/chi/outputDataFirst");
        T a= (T) LabeledPoint$.MODULE$.parse(first);
        return a;
    }

    @Override
    public ApexRDD<T>[] randomSplit(double[] weights, long seed){
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(dag);
        MyDAG cloneDag2= (MyDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        RandomSplitOperator randomSplitOperator = cloneDag.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        randomSplitOperator.weights=weights;
        //cloneDag.setInputPortAttribute(randomSplitOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentOutputPort, randomSplitOperator.input);
        DefaultOutputPortSerializable currentSplitOutputPort2 = getCurrentOutputPort(cloneDag2);
        RandomSplitOperator randomSplitOperator2 = cloneDag2.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        randomSplitOperator2.weights=weights;
        randomSplitOperator2.flag=true;
        //cloneDag2.setInputPortAttribute(randomSplitOperator2.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag2.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentSplitOutputPort2, randomSplitOperator2.input);
        ApexRDD temp1 = (ApexRDD) SerializationUtils.clone(this);
        temp1.dag=cloneDag;
        ApexRDD temp2=(ApexRDD) SerializationUtils.clone(this);
        temp2.dag=cloneDag2;
        ApexRDD[] temp=new ApexRDD[]{temp1, temp2};
        return temp;
    }

    @Override
    public <U> RDD<U> mapPartitions(Function1<Iterator<T>, Iterator<U>> f, boolean preservesPartitioning, ClassTag<U> evidence$6) {
        MyDAG cloneDag = (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapPartitionOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " MapPartitionOperator " , new MapPartitionOperator());
        cloneDag.addStream( System.currentTimeMillis()+ " MapPartitionsStream ", currentOutputPort, m1.input);
        //cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        m1.f= f;
        ApexRDD<U> temp = (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag = cloneDag;
        return temp;
    }

    @Override
    public <U> U withScope(Function0<U> body) {
        return RDDOperationScope$.MODULE$.withScope(context,false,body);
    }

    @Override
    public Map<T, Object> countByValue(Ordering<T> ord) {
        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        CountByVlaueOperator countByVlaueOperator =cloneDag.addOperator(System.currentTimeMillis()+" CountByVlaueOperator",CountByVlaueOperator.class);
        cloneDag.addStream(System.currentTimeMillis()+" CountValue Stream",currentOutputPort,countByVlaueOperator.getInputPort());
        //cloneDag.setInputPortAttribute(countByVlaueOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        controlOutput= getControlOutput(cloneDag);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, countByVlaueOperator.controlDone);
        ObjectFileWriterOperator fileWriterOperator=cloneDag.addOperator(System.currentTimeMillis()+ "WriteMap ",new ObjectFileWriterOperator());
        fileWriterOperator.setAbsoluteFilePath("/harsh/chi/countByValueOutput");
        cloneDag.addStream(System.currentTimeMillis()+" MapWrite", countByVlaueOperator.output, fileWriterOperator.input);
        cloneDag.validate();
        log.info("DAG successfully validated CountByValue");

        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        ApexDoTask apexDoTask = new ApexDoTask();
        try {
            apexDoTask.launch(app,countByValueApp,RDDLIBJARS);
        } catch (Exception e) {
            e.printStackTrace();
        }

        HashMap hashMap= ObjectReader("hdfs://localhost:54310/harsh/chi/countByValueOutput");
        Map<T, Object> map = this.scalaMap(hashMap);
        return map;
    }



    @Override
    public T[] collect() {
        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        CollectOperator collectOperator =cloneDag.addOperator(System.currentTimeMillis()+" Collect Operator",CollectOperator.class);
        //cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Collect Stream",currentOutputPort,collectOperator.input);
        cloneDag.validate();

        log.info("DAG successfully validated");

        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        ApexDoTask apexDoTask = new ApexDoTask();
        try {
            apexDoTask.launch(app,"ChiCollect",RDDLIBJARS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        T[] array= (T[]) CollectOperator.t.toArray();

        return array;
    }

    public void foreach(Function<LabeledPoint, LabeledPoint> function) {
        MyDAG cloneDag= (MyDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        ForeachOpeator foreach = cloneDag.addOperator(System.currentTimeMillis()+" ForEachOperator",new ForeachOpeator());

        foreach.f= function;
        cloneDag.addStream(System.currentTimeMillis()+" ForEachStream", currentOutputPort, foreach.input);
        SimpleFileWriteOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", new SimpleFileWriteOperator());
        //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("/home/harsh/apex-integration/spark-apex/spark-example/src/main/resources/data/transformer/filterData");
        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", foreach.output, writer.input);
        cloneDag.validate();
        log.info("DAG successfully validated CountByValue");

        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        ApexDoTask apexDoTask = new ApexDoTask();
        try {
            apexDoTask.launch(app,"ChiForeach",RDDLIBJARS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public enum OperatorType {
        INPUT,
        PROCESS,
        OUTPUT
    }
}
