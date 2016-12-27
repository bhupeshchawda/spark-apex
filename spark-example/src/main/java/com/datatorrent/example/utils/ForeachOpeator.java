package com.datatorrent.example.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.function.Function;
import scala.Function1;

import java.io.Serializable;

/**
 * Created by harsh on 27/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class ForeachOpeator<T> extends MyBaseOperator<T> implements Serializable {
    public ForeachOpeator(){}
    public Function f;
    Logger log = LoggerFactory.getLogger(ForeachOpeator.class);
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            //print filtered data
            try {
                log.info("tuple {}  class {} apply {} ",tuple, tuple.getClass(), String.valueOf((f.call(tuple))));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Override
    public DefaultInputPortSerializable getInputPort() {
        return this.input;
    }

    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return null;
    }

    @Override
    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}
