package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import scala.Function1;

import java.io.Serializable;
@DefaultSerializer(JavaSerializer.class)
public class MapOperator extends MyBaseOperator implements Serializable
{

  public Function1 f;
  public DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();

  public   DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
    @Override
    public void process(Object tuple)
    {
//      System.out.println("Tuple :" +tuple);
      output.emit(f.apply(tuple));
    }
  };

  public DefaultOutputPortSerializable<Object> getOutputPort(){
    return this.output;
  }

  public DefaultInputPortSerializable getControlPort() {
    return null;
  }

  public DefaultOutputPortSerializable<Boolean> getControlOut() {
    return null;
  }

  public DefaultInputPortSerializable<Object> getInputPort(){
    return this.input;
  }
  public boolean isInputPortOpen=true;
  public boolean isOutputPortOpen=true;
}
