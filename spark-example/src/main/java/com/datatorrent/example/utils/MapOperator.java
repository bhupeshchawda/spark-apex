package com.datatorrent.example.utils;

import com.datatorrent.example.MyBaseOperator;
import scala.Function1;

public class MapOperator extends MyBaseOperator
{

  public Function1 f;
  public final transient DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
    @Override
    public void process(Object tuple)
    {
      output.emit(f.apply(tuple));
    }
  };
  public final transient DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
  public DefaultOutputPortSerializable<Object> getOutputPort(){
    return this.output;
  }

  public DefaultOutputPortSerializable getControlPort() {
    return null;
  }

  public DefaultInputPortSerializable<Object> getInputPort(){
    return this.input;
  }
  public boolean isInputPortOpen=true;
  public boolean isOutputPortOpen=true;
}
