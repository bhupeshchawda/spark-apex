package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by anurag on 12/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class CollectOperator<T> extends MyBaseOperator implements Serializable {
    Logger log = LoggerFactory.getLogger(CollectOperator.class);
    int count;
    public  ArrayList<Object> dataList;


    @Override
    public void beginWindow(long windowId) {
        count=0;
    }

    @Override
    public void endWindow() {
        if(count==0)
            output.emit(dataList);
    }
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(Object tuple) {
            dataList.add(tuple);
            count++;
        }
    };

    @Override
    public void setup(Context.OperatorContext context) {
        dataList=new ArrayList<>();
    }
    public DefaultOutputPortSerializable output= new DefaultOutputPortSerializable();

    public CollectOperator(){}

    @Override
    public DefaultInputPortSerializable getInputPort() {
        return this.input;
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
