package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by anurag on 4/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class PersistOperator extends MyBaseOperator implements Serializable {
    public PersistOperator(){

    }
    public final  DefaultInputPortSerializable<Object>   input= new DefaultInputPortSerializable<Object>() {
        @Override
        public void process(Object tuple) {

        }
    };
    public final DefaultOutputPortSerializable<Object> output=new DefaultOutputPortSerializable<Object>();
    public DefaultInputPortSerializable<Object> getInputPort() {
        return input;
    }

    public DefaultOutputPortSerializable getOutputPort() {
        return output;
    }

    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}
