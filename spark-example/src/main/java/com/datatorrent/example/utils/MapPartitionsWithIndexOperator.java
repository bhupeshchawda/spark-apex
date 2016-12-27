package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import scala.Function2;
import scala.collection.Iterator;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by krushika on 27/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class MapPartitionsWithIndexOperator<U,T> extends MyBaseOperator implements Serializable{
    public Function2<Object, Iterator<T>, Iterator<U>> f;
    public Iterator<U> result;
    public Iterator<T> iterator;
    public ArrayList<T> rddData = new ArrayList<T>();
    public Boolean preservePartitioning;
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            rddData.add(tuple);
        }
    };

    @Override
    public void endWindow() {
        iterator = scala.collection.JavaConversions.asScalaIterator(rddData.listIterator());
       result =  f.apply(1,iterator);
        while(result.hasNext()){
            output.emit(result.next());
        }
    }

    public DefaultOutputPortSerializable<U> output = new DefaultOutputPortSerializable<U>();
    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return null;
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
