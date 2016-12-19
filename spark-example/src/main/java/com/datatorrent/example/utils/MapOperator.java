package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.Serializable;
import org.apache.spark.api.java.function.*;
import scala.Tuple3;

@DefaultSerializer(JavaSerializer.class)
public class MapOperator<T,U> extends MyBaseOperator implements Serializable {
    public int ID;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        ID=context.getId();

    }
    Logger log = LoggerFactory.getLogger(MapOperator.class);
    public Function1 f;
    public Function ff;

    public DefaultOutputPortSerializable<U> output = new DefaultOutputPortSerializable<U>();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
                try {
                    output.emit((U) f.apply(tuple));
                    log.info("Function applied on tuple {} of OperatorID {} at Map",tuple,ID);
                }
                catch (Exception e){
                    log.info("Exception Occured Due to {} of OperatorID {} at Map",tuple,ID);
                    output.emit((U)tuple);
                    e.printStackTrace();
                }

        }
    };


    public DefaultOutputPortSerializable<U> getOutputPort() {
        return this.output;
    }

    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }

    public DefaultInputPortSerializable<Object> getInputPort() {
        return (DefaultInputPortSerializable<Object>) this.input;
    }

    public boolean isInputPortOpen = true;
    public boolean isOutputPortOpen = true;
}
