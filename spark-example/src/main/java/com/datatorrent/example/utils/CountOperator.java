package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;
@DefaultSerializer(JavaSerializer.class)
public class CountOperator extends MyBaseOperator implements Serializable
{
    private boolean done = false;

    public CountOperator() {
    }

    public DefaultOutputPortSerializable<Integer> getCountOutputPort() {
        return null;
    }

    @Override
    public void beginWindow(long windowId)
    {
        if (done) {
            output.emit(count);
        }
    }
    Integer count =0;
    public final  DefaultInputPortSerializable<Object>   input = new DefaultInputPortSerializable<Object>() {
        @Override
        public void process(Object tuple)
        {
            count++;
        }
    };

    public final  DefaultInputPortSerializable<Boolean> controlDone = new DefaultInputPortSerializable<Boolean>() {
        @Override
        public void process(Boolean tuple)
        {
            done = true;
        }
    };
    public final  DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
    public  DefaultOutputPortSerializable<Object> getOutputPort(){
        return this.output;
    }

    public DefaultInputPortSerializable getControlPort() {
        return controlDone;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }

    public DefaultInputPortSerializable<Object> getInputPort(){
        return this.input;
    }

}
