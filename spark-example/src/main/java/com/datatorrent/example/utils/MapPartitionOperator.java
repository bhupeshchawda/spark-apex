package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function3;
import scala.collection.Iterator;

import java.io.Serializable;

@DefaultSerializer(JavaSerializer.class)
public class MapPartitionOperator<T,U> extends MyBaseOperator implements Serializable {
    int id=0;
    public TaskContext taskContext;
    public Object object;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        id=context.getId();
    }
    Logger log = LoggerFactory.getLogger(MapOperator.class);
    public Function3<TaskContext, Object, Iterator<T>, Iterator<U>> f;
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();
    public DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
        @Override
        public void process(Object tuple) {
            try {
                output.emit(f.apply(taskContext,object, (Iterator<T>) tuple));
            } catch (Exception e){
                log.info("Exception Occured Due to {} ",tuple.getClass());
                e.printStackTrace();
//                output.emit(tuple);
            }
        }
    };


    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return null;
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


    public boolean isInputPortOpen = true;
    public boolean isOutputPortOpen = true;
}
