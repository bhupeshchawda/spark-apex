package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.Serializable;
@DefaultSerializer(JavaSerializer.class)
public class FilterOperator extends MyBaseOperator implements Serializable
{
  public Function1 f;
  public int ID;
  Logger log = LoggerFactory.getLogger(MapOperator.class);
  public final  DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
    @Override
    public void process(Object tuple)
    {
      if((Boolean) f.apply(tuple)) {
        output.emit(tuple);
        log.info("Function applied on tuple {} of OperatorID {} in Filter",tuple,ID);
      }
    }
  };

  @Override
  public void setup(Context.OperatorContext context) {
    super.setup(context);
    ID=context.getId();
  }

  public final  DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
  public DefaultOutputPortSerializable<Object> getOutputPort(){
    return this.output;
  }

  public DefaultInputPortSerializable getControlPort() {
    return null;
  }

  public DefaultOutputPortSerializable<Boolean> getControlOut() {
    return null;
  }

    public DefaultOutputPortSerializable<Integer> getCountOutputPort() {
        return null;
    }

    public DefaultInputPortSerializable<Object> getInputPort(){
    return this.input;
  }

}

