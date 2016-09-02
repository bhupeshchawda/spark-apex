package com.datatorrent.example.utils;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import scala.Function2;

public class ReduceOperator extends BaseOperator
{
  public Function2 f;
  Object previousValue = null;
  Object finalValue = null;
  private boolean done = false;

  @Override
  public void beginWindow(long windowId)
  {
    if (done) {
      output.emit(finalValue);
    }
  }

  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
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

  public final transient DefaultInputPort<Boolean> controlDone = new DefaultInputPort<Boolean>()
  {
    @Override
    public void process(Boolean tuple)
    {
      done = true;
    }
  };
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();
}
