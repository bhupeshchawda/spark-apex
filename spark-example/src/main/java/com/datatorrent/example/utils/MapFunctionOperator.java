package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by krushika on 19/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class MapFunctionOperator<T> extends MyBaseOperator implements Serializable {

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);

    }
    Logger log = LoggerFactory.getLogger(MapOperator.class);
    public Function f;
    public DefaultOutputPortSerializable<T> output = new DefaultOutputPortSerializable<T>();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            try {

                output.emit((T) f.call(tuple));
            }
            catch (Exception e){
                output.emit(tuple);
            }

        }
    };


    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable<T> getOutputPort() {return this.output;}

    @Override
    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}
