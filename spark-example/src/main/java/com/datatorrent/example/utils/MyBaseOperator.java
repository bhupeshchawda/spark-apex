package com.datatorrent.example.utils;

import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by harsh on 2/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public abstract class MyBaseOperator extends BaseOperator implements  Serializable{
    public MyBaseOperator(){}
    public abstract DefaultInputPortSerializable<Object> getInputPort();
    public abstract DefaultOutputPortSerializable getOutputPort();
    public  abstract DefaultInputPortSerializable getControlPort();
    public  abstract DefaultOutputPortSerializable<Boolean> getControlOut();


}
