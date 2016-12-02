package com.datatorrent.example.utils;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import scala.Function1;

public class FilterOperator extends BaseOperator
{
  public Function1 f;
  public final transient DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
    @Override
    public void process(Object tuple)
    {

      if((Boolean) f.apply(tuple)) {
        output.emit(tuple);
      }
    }
  };
  public final transient DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
  public DefaultOutputPort<Object> getOutputPort(){
    return this.output;
  }
  public DefaultInputPort<Object> getInputPort(){
    return this.input;
  }
  public boolean isInputPortOpen=true;
  public boolean isOutputPortOpen=true;

}

