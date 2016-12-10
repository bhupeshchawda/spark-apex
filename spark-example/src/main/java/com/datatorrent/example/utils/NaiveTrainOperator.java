package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;

/**
 * Created by harsh on 10/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class NaiveTrainOperator extends MyBaseOperator implements Serializable {
    Logger log = LoggerFactory.getLogger(NaiveTrainOperator.class);
    public DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
        public void process(Object tuple) {
            log.info("We are in NaiveTrain operator"+ tuple);
        }
    };
    public DefaultInputPortSerializable<Object> getInputPort() {
        return input;
    }

    public DefaultOutputPortSerializable getOutputPort() {
        return null;
    }

    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}
