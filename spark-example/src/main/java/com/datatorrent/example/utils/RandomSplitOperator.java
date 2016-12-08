package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by harsh on 8/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class RandomSplitOperator extends MyBaseOperator implements Serializable {
    public boolean done= false;
    public DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
        @Override
        public void process(Object tuple) {

            if(done){
                output.emit(tuple);
            }

        }
    };

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);

    }

    public DefaultInputPortSerializable<Boolean> controlDone= new DefaultInputPortSerializable<Boolean>() {
        @Override
        public void process(Boolean tuple) {
            done =true;
        }
    };
    public DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
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
