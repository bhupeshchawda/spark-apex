package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function3;
import scala.collection.Iterable;
import scala.collection.Iterator;
import scala.collection.JavaConversions$;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.ListIterator;

@DefaultSerializer(JavaSerializer.class)
public class MapPartitionOperator<T,U> extends MyBaseOperator implements Serializable {
    int ID;
    public TaskContext taskContext;
    public Object object;
    public MapPartitionOperator(){}
    Logger log = LoggerFactory.getLogger(MapPartitionOperator.class);
    public Function1 f;
    public DefaultOutputPortSerializable output = new DefaultOutputPortSerializable();
    public Iterator<T> iterT;
    ArrayList<T> rddData = new ArrayList<>();

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        ID=context.getId();
    }

    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            /*String[] a = tuple.toString().split(",");
            Double[] b = new Double[a.length];
            for(int i=3;i<a.length-1;i++){
                b[i]= Double.parseDouble(a[i]);
            }
            ListIterator list = new ArrayListIterator(b);
            iterT =JavaConversions$.MODULE$.asScalaIterator(list);
            try {
                Iterator<U> iterU = (Iterator<U>) f.apply(iterT);
                while (iterU.hasNext())
                    output.emit(iterU.next());*/
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