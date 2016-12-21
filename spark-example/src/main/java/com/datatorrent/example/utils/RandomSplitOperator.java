package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by harsh on 8/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class RandomSplitOperator extends MyBaseOperator implements Serializable {

    public double[] weights;

    public  boolean flag=false;

    public int limit;
    public int a,b;
    public long count= 1;


    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
            weights[0]=weights[0]*count;
            weights[1]=weights[1]*count;
            a= (int) Math.ceil(count/weights[0]);
            b= (int) Math.ceil(count/weights[1]);

    }

    public boolean done= false;
    private int index=0;
    Logger log = LoggerFactory.getLogger(RandomSplitOperator.class);

    public DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
        @Override
        public void process(Object tuple) {
            index++;
            if(index%a==0 && !flag){
                output.emit(tuple);
            }
            else if(index%a!=0 && flag){
                output.emit(tuple);
            }

        }
    };



    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
    }


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
