package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.Serializable;
import org.apache.spark.api.java.function.*;

@DefaultSerializer(JavaSerializer.class)
public class MapFunctionOperator<T> extends MyBaseOperator implements Serializable {

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);

    }
    Logger log = LoggerFactory.getLogger(MapOperator.class);
    public Function1 f;
    public Function ff;
    public DefaultOutputPortSerializable<T> output = new DefaultOutputPortSerializable<T>();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            try {
                output.emit((T) ff.call(tuple));
            }
            catch (Exception e){
               // log.info("Exception Occured Due to {} ",tuple);
                output.emit(tuple);
              //  e.printStackTrace();
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

    public DefaultInputPortSerializable<Object> getInputPort() {
        return (DefaultInputPortSerializable<Object>) this.input;
    }
}
