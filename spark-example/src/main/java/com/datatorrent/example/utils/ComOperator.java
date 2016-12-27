package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import scala.Function2;
import java.io.Serializable;
import java.util.ArrayList;


/**
 * Created by krushika on 26/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class ComOperator<U> extends MyBaseOperator implements Serializable {
    public ComOperator(){ }
    public ArrayList<U> result = new ArrayList<>();
    public U zeroValue;
    public Function2 f;
    public DefaultOutputPortSerializable<U> output = new DefaultOutputPortSerializable<U>();
    public DefaultInputPortSerializable<U> input = new DefaultInputPortSerializable<U>() {

        @Override
        public void process(U tuple) {
//            output.emit((U) f.apply(tuple,zeroValue));
            result.add((U) f.apply(tuple,zeroValue));
        }
    };


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
