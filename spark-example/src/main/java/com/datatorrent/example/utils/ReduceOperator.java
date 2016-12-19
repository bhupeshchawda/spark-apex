package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function2;

import java.io.Serializable;
@DefaultSerializer(JavaSerializer.class)
public class ReduceOperator extends MyBaseOperator implements Serializable
{
  public Function2 f;
  public Object previousValue = null;
  public Object finalValue = null;
  private boolean done = false;
  public int ID;
  public ReduceOperator() {}

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
  Logger log = LoggerFactory.getLogger(MapOperator.class);
  @Override
  public void setup(Context.OperatorContext context) {
    super.setup(context);
    ID=context.getId();
  }

  public final  DefaultInputPortSerializable<Object>   input = new DefaultInputPortSerializable<Object>() {
    @Override
    public void process(Object tuple)
    {
      if (previousValue == null) {
        previousValue = tuple;
        finalValue = tuple;
      } else {
          previousValue = tuple;
          finalValue = f.apply(finalValue, previousValue);
      }
      log.info("ReduceOperator operator ID {}",ID);


    }
  };

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

  public DefaultInputPortSerializable<Object> getInputPort(){
    return this.input;
  }
  public boolean isInputPortOpen=true;
  public boolean isOutputPortOpen=true;
  public boolean isControlInputOpen=true;
  public boolean isControlOutputOpen=true;

}
