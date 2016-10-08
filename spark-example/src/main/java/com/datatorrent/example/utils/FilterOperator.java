package com.datatorrent.example.utils;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import scala.Function1;

public class FilterOperator extends BaseOperator
{
  public Function1 f;
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      if((Boolean) f.apply(tuple)) {
        output.emit(tuple);
      }
    }
  };
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();
  public DefaultOutputPort<Object> getOutputPort(){
    return this.output;
  }
  public DefaultInputPort<Object> getInputPort(){
    return this.input;
  }
  public boolean isInputPortOpen=true;
  public boolean isOutputPortOpen=true;

}

