package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by krushika on 22/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class ZipOperator<T,U> extends MyBaseOperator implements Serializable {
    public ZipOperator(){}
    public List<T> other;
    int i;
    public int count;

    @Override
    public void setup(Context.OperatorContext context) {

        i=0;
    }

    public DefaultOutputPortSerializable<Tuple2<T,U>> output = new DefaultOutputPortSerializable<Tuple2<T, U>>();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            if(i<=count) {
                Tuple2<T,U> zipped = new Tuple2<T,U>(tuple, (U) other.get(i));
                output.emit(zipped);
                i++;
            }
        }
    };
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
