package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.mllib.linalg.SparseVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.Serializable;

@DefaultSerializer(JavaSerializer.class)
public class MapPartitionOperator<T> extends MyBaseOperator implements Serializable {
    int id=0;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        id=context.getId();
    }
    Logger log = LoggerFactory.getLogger(MapOperator.class);
    public Function1 f;
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            try {
                SparseVector v = (SparseVector) tuple;
                output.emit(f.apply(((SparseVector) tuple).asML()));
            } catch (Exception e){
                log.info("Exception Occured Due to {} ",tuple.getClass());
                e.printStackTrace();
//                output.emit(tuple);
            }
        }
    };


    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return null;
    }

    public DefaultOutputPortSerializable getOutputPort() {
        return this.output;
    }

    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }


    public boolean isInputPortOpen = true;
    public boolean isOutputPortOpen = true;
}
