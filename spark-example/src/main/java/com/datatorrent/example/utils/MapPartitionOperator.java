package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function3;
import scala.collection.Iterator;
import scala.collection.JavaConversions$;

import java.io.Serializable;
import java.util.ListIterator;

@DefaultSerializer(JavaSerializer.class)
public class MapPartitionOperator<T,U> extends MyBaseOperator implements Serializable {
    int ID;
    public TaskContext taskContext;
    public Object object;
    public MapPartitionOperator(){}
    Logger log = LoggerFactory.getLogger(MapPartitionOperator.class);
    public Function3<TaskContext, Object, Iterator<T>, Iterator<U>> f;
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();
    public Iterator<T> iterT;

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        ID=context.getId();
    }

    public DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
        @Override
        public void process(Object tuple) {
            try {
                String[] a = tuple.toString().split(",");
                ListIterator list = new ArrayListIterator(a);
                iterT =JavaConversions$.MODULE$.asScalaIterator(list);
                Iterator<U> iterU = f.apply(taskContext,object, iterT);
                output.emit(iterU);
                log.info(String.valueOf((f.apply(taskContext,object, iterT)).getClass()));
            } catch (Exception e){
                log.info("Exception Occured Due to {}  {} of OperatorID {} in MapPartition",tuple,tuple.getClass(),ID);
                e.printStackTrace();
            }
        }
    };


    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return this.input;
    }

    public DefaultOutputPortSerializable getOutputPort() {
        return this.output;
    }

    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}