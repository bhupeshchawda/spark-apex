package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Created by harsh on 8/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class RandomSplitOperator<T> extends MyBaseOperator implements Serializable {

    public double[] weights;

    public  boolean flag=false;
    public static BitSet bitSet;
    public int limit;
    public int a,b;
    public long count= 1;


    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        limit= (int) (count*weights[0]);

    }

    public boolean done= false;
    private int index=0;
    Logger log = LoggerFactory.getLogger(RandomSplitOperator.class);

    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {

            if( Math.random()<=weights[0] && index<limit && !flag){
                bitSet.set(index);
                output.emit(tuple);
            }
            else if( flag  && !bitSet.get(index)){

                output.emit(tuple);
            }
            index++;

        }
    };



    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
    }


    public DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
    public DefaultInputPortSerializable<T> getInputPort() {
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
