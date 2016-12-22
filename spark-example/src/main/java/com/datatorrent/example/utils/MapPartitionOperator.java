package com.datatorrent.example.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by harsh on 22/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class MapPartitionOperator<T> extends MyBaseOperator implements Serializable {
    MapPartitionOperator(){}
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            System.out.println(tuple.getClass());
        }
    };
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();
    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return (DefaultInputPortSerializable<Object>) this.input;
    }


    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return this.output;
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
