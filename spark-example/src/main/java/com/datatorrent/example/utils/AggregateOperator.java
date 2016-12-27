package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import scala.Function2;

import java.io.Serializable;

/**
 * Created by krushika on 23/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class AggregateOperator<T,U> extends MyBaseOperator implements Serializable {

    public Function2<U,T,U> seqFunction;
    public Function2<U,U,U> combFunction;
    public static Object aggregateValue;
    public U zeroValue;
    private U seqData;

    public DefaultOutputPortSerializable<U> output = new DefaultOutputPortSerializable<U>();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            seqData = seqFunction.apply(zeroValue,tuple);
        }
    };

    @Override
    public void endWindow() {
        aggregateValue = combFunction.apply(seqData,zeroValue);
        output.emit((U) aggregateValue);
    }

    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {return (DefaultInputPortSerializable<Object>) this.input;}

    @Override
    public DefaultOutputPortSerializable getOutputPort() {return this.output;}

    @Override
    public DefaultInputPortSerializable getControlPort() {return null;}

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {return null;}
}
