package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.Serializable;

@DefaultSerializer(JavaSerializer.class)
public class MapOperator<T,U> extends MyBaseOperator implements Serializable {
    int id=0;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
    }
    Logger log = LoggerFactory.getLogger(MapOperator.class);
    public Function1<T,U> f;
    public DefaultOutputPortSerializable<U> output = new DefaultOutputPortSerializable<>();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {

                try {
                    output.emit( f.apply(tuple));
                } catch (Exception e){
                    log.debug("Exception Occured Due to {} ",tuple);

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

    public DefaultInputPortSerializable<T> getInputPort() {
        return this.input;
    }

    public boolean isInputPortOpen = true;
    public boolean isOutputPortOpen = true;
}
