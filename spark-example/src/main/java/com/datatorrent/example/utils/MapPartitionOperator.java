package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.collection.Iterator;

import java.io.Serializable;
import java.util.ArrayList;

@DefaultSerializer(JavaSerializer.class)
public class MapPartitionOperator<T,U> extends MyBaseOperator implements Serializable {
    int ID;
    public Object object;
    public MapPartitionOperator(){}
    Logger log = LoggerFactory.getLogger(MapPartitionOperator.class);
    public Function1 f;
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();
    ArrayList<T> rddData;

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        ID=context.getId();
        rddData = new ArrayList<>();
    }

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
    }

    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            try{
                rddData.add(tuple);
            } catch (Exception e){
                log.info("Exception Occured Due to {}  {} of OperatorID {} in MapPartition",tuple,tuple.getClass(),ID);
                e.printStackTrace();
            }
        }
    };

    @Override
    public void endWindow() {
        super.endWindow();
        Iterator<U> iterU = (Iterator<U>) f.apply(scala.collection.JavaConversions.asScalaIterator(rddData.iterator()));
        while (iterU.hasNext())
            output.emit(iterU.next());
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
}