package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function3;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;

import java.io.Serializable;
import java.util.ArrayList;

@DefaultSerializer(JavaSerializer.class)
public class MapPartitionOperator<T,U> extends MyBaseOperator implements Serializable {
    int id=0;
    List<T>  rddList= List$.MODULE$.empty();
    ArrayList<T> rddData = new ArrayList<>();
    public TaskContext taskContext;
    public Object object;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        id=context.getId();
    }
    Logger log = LoggerFactory.getLogger(MapPartitionOperator.class);
    public Function3 f;
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            try {
                rddData.add(tuple);
            } catch ( Exception e){
                log.info("Exception Occured Due to {} ",tuple.getClass());
                e.printStackTrace();
//                output.emit(tuple);
            }
        }
    };

    @Override
    public void endWindow() {
        output.emit(f.apply(taskContext, 0,
                scala.collection.JavaConversions.asScalaIterator(rddData.iterator())));
    }

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
