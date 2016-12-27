package com.datatorrent.example.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.Serializable;

/**
 * Created by harsh on 27/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class ForeachOpeator<T> extends MyBaseOperator<T> implements Serializable {
    public ForeachOpeator(){}
    public Function1 f;
    Logger log = LoggerFactory.getLogger(MapOperator.class);
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
           log.info("tuple {}  class {} apply {} ",tuple, tuple.getClass(), String.valueOf(f.apply(tuple)));
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
