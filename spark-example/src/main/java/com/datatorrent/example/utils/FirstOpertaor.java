package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by harsh on 17/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class FirstOpertaor<T> extends MyBaseOperator implements Serializable {
    private boolean flag =true;
    public  T a ;
    public DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {

        @Override
        public void process(Object tuple) {
            if(flag) {
                output.emit((T)tuple);
                a= (T)tuple;
                flag=false;
            }
        }
    };
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();

    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return input;
    }

    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return output;
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
