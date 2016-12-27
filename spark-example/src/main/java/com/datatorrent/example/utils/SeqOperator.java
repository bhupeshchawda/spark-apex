package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import scala.Function2;


import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by krushika on 26/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class SeqOperator<U,T> extends MyBaseOperator implements Serializable {
    public SeqOperator (){}
    public Function2<U,T,U> f;
    public U zeroValue;
    private Boolean done = false;
    public DefaultOutputPortSerializable<U> output = new DefaultOutputPortSerializable<U>();

    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {

            output.emit(f.apply(zeroValue,tuple));
        }


    };
//
//    public final  DefaultInputPortSerializable<Boolean> controlDone = new DefaultInputPortSerializable<Boolean>() {
//        @Override
//        public void process(Boolean tuple)
//        {
//            done = true;
//        }
//    };

    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return null;
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
