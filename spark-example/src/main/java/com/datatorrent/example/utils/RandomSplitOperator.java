package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Random;

/**
 * Created by harsh on 8/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class RandomSplitOperator<T> extends MyBaseOperator implements Serializable {

    public double[] weights;

    public  boolean flag;
    public static BitSet bitset;
    public int limit;
    public long count;
    private int index;


    @Override
    public void setup(Context.OperatorContext context) {
        index=0;
        limit= (int) (count*weights[0]);

    }
    int temp;
    @Override
    public void beginWindow(long windowId) {
        temp=0;

    }

    @Override
    public void endWindow() {
        if(temp==0){
            System.out.println("Index "+bitset.toString());
        }
    }

    public boolean done= false;
    Logger log = LoggerFactory.getLogger(RandomSplitOperator.class);
    public int getRandom(){
        Random random =new Random();
        return random.nextInt((int) (count+1));
    }
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            temp++;
            if(!flag && index<limit){
                int randomIndex=getRandom();
                while(bitset.get(randomIndex))
                    randomIndex=getRandom();
                bitset.set(randomIndex);
                output.emit(tuple);

            }

            else if(flag && !bitset.get(index)){
                System.out.println(index);
                output.emit(tuple);
            }
            index++;
        }
    };





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
