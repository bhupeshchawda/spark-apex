package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by anurag on 28/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class TakeOperator extends MyBaseOperator implements Serializable {
    public TakeOperator(){}

    public static ArrayList<Object> elements ;
    public static  int count;


    @Override
    public void setup(Context.OperatorContext context) {
        elements=  new ArrayList<>();
    }

    @Override
    public void beginWindow(long windowId) {

    }
    public DefaultOutputPortSerializable output= new DefaultOutputPortSerializable();
    public DefaultInputPortSerializable input =new DefaultInputPortSerializable() {
        @Override
        public void process(Object tuple) {
            if(count!=0){
                elements.add(tuple);
            }
        }
    };

    @Override
    public void endWindow() {
        output.emit(elements);
    }

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
