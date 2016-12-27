package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.TaskContext;
import scala.Function1;
import scala.Function2;

import java.io.Serializable;
import java.util.ArrayList;

@DefaultSerializer(JavaSerializer.class)
public class ReduceOperator<T> extends MyBaseOperator implements Serializable
{
  public Function2<T,T,T> f;
  public Function1 f1;
  public T previousValue = null;
  public static Object finalValue;
  private boolean done = false;
    ArrayList<T> rddData = new ArrayList<>();
    public TaskContext taskContext;
    public Object object;
  public ReduceOperator() {

  }

    public DefaultOutputPortSerializable<Integer> getCountOutputPort() {
        return null;
    }

    @Override
  public void beginWindow(long windowId)
  {
    if (done) {
      output.emit(finalValue);
    }
  }


    public final  DefaultInputPortSerializable<T>   input = new DefaultInputPortSerializable<T>() {
    @Override
    public void process(T tuple)
    {
      if (previousValue == null) {
        previousValue = tuple;
        finalValue = tuple;
      } else {
          previousValue = tuple;
          try {
              finalValue = f.apply((T) finalValue, previousValue);
          }
          catch (ClassCastException classCastException){

//              finalValue= (T) f1.apply(previousValue);

            classCastException.printStackTrace();
          }
      }

    }
  };
    @Override
    public void endWindow() {
//        output.emit(f3.apply(taskContext, 0,
//                scala.collection.JavaConversions.asScalaIterator(rddData.iterator())));
    }

  public final  DefaultInputPortSerializable<Boolean> controlDone = new DefaultInputPortSerializable<Boolean>() {
    @Override
    public void process(Boolean tuple)
    {
      done = true;
    }
  };
  public final  DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
  public  DefaultOutputPortSerializable<Object> getOutputPort(){
    return this.output;
  }

  public DefaultInputPortSerializable getControlPort() {
    return controlDone;
  }

  public DefaultOutputPortSerializable<Boolean> getControlOut() {
    return null;
  }

  public DefaultInputPortSerializable<T> getInputPort(){
    return  this.input;
  }


}
