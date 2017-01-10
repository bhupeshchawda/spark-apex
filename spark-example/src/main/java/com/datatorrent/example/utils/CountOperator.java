package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;
@DefaultSerializer(JavaSerializer.class)
public class CountOperator<T> extends MyBaseOperator implements Serializable
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
    Long count =0L;
    public final  DefaultInputPortSerializable<T>   input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple)
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

    public DefaultInputPortSerializable<T> getInputPort(){
        return this.input;
    }

}
