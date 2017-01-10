package com.datatorrent.example.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by harsh on 17/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class FirstOperator<U> extends MyBaseOperator implements Serializable {
    private boolean flag =true;
    public LabeledPoint a;
    Logger log = LoggerFactory.getLogger(FirstOperator.class);
    public DefaultInputPortSerializable<U> input = new DefaultInputPortSerializable<U>() {

        @Override
        public void process(U tuple) {
            if(flag) {
                output.emit((U)tuple);
                a= (LabeledPoint) tuple;
                //log.info("First tuple {}", tuple);
                flag=false;
            }
        }
    };
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();

    @Override
    public DefaultInputPortSerializable getInputPort() {
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
