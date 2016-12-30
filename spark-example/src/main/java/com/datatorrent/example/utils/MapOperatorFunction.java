package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

@DefaultSerializer(JavaSerializer.class)
public class MapOperatorFunction<T> extends MyBaseOperator implements Serializable {
    int id=0;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        id=context.getId();

    }
    Logger log = LoggerFactory.getLogger(MapOperatorFunction.class);
    public Function f;
    public DefaultOutputPortSerializable<T> output = new DefaultOutputPortSerializable<T>();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            try {

                output.emit((T) f.call(tuple));
            }
            catch (Exception e){
                e.printStackTrace();
                log.debug("Exception Occured Due to {} ",tuple);
                output.emit(tuple);
            }

        }
    };


    public DefaultOutputPortSerializable<T> getOutputPort() {
        return this.output;
    }

    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }

    public DefaultInputPortSerializable<T> getInputPort() {
        return  this.input;
    }

    public boolean isInputPortOpen = true;
    public boolean isOutputPortOpen = true;
}
