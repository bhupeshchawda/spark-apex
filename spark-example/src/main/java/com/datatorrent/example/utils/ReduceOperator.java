package com.datatorrent.example.utils;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import scala.Function2;

import java.io.Serializable;

public class ReduceOperator extends BaseOperator implements Serializable
{
  public Function2 f;
  public Object previousValue = null;
  public Object finalValue = null;
  private boolean done = false;

  @Override
  public void beginWindow(long windowId)
  {
    if (done) {
      output.emit(finalValue);
    }
  }

  public final transient DefaultInputPortSerializable<Object>   input = new DefaultInputPortSerializable<Object>() {
    @Override
    public void process(Object tuple)
    {
      if (previousValue == null) {
        previousValue = tuple;
        finalValue = tuple;
      } else {
        finalValue = f.apply(finalValue, previousValue);

      }
    }
  };

  public final transient DefaultInputPortSerializable<Boolean> controlDone = new DefaultInputPortSerializable<Boolean>() {
    @Override
    public void process(Boolean tuple)
    {
      done = true;
    }
  };
  public final transient DefaultOutputPortSerializable<Object> output = (DefaultOutputPortSerializable<Object>) new DefaultOutputPort<Object>();
  public DefaultOutputPortSerializable<Object> getOutputPort(){
    return this.output;
  }
  public DefaultInputPortSerializable<Object> getInputPort(){
    return this.input;
  }
  public boolean isInputPortOpen=true;
  public boolean isOutputPortOpen=true;
  public boolean isControlInputOpen=true;
  public boolean isControlOutputOpen=true;

}
