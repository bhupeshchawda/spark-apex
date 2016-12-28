package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by harsh on 21/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class CountByVlaueOperator<K,V> extends MyBaseOperator implements Serializable {

    public CountByVlaueOperator() {

    }

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
    }

    public final  DefaultInputPortSerializable<Boolean> controlDone = new DefaultInputPortSerializable<Boolean>() {
        @Override
        public void process(Boolean tuple)
        {
            done = true;
        }
    };

    Logger log = LoggerFactory.getLogger(CountByVlaueOperator.class);
    public static HashMap<Object, Long> hashMap;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        hashMap= new HashMap<>();
    }
    public DefaultInputPortSerializable input = new DefaultInputPortSerializable() {
        @Override
        public void process(Object tuple) {
            {
                if(hashMap.containsKey(tuple)) {
                    long x= hashMap.get(tuple).longValue();
                    hashMap.put(tuple, new Long(x+1));
                }
                else {
                    hashMap.put(tuple, (long) 1.0);
                }
            }
        }
    };
    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return input;
    }
    private boolean done = false;


    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return null;
    }

    @Override
    public DefaultInputPortSerializable getControlPort() {
        return controlDone;
    }

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}
