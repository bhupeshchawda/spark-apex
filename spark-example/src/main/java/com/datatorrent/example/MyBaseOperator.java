package com.datatorrent.example;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.example.utils.DefaultInputPortSerializable;
import com.datatorrent.example.utils.DefaultOutputPortSerializable;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by harsh on 2/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public abstract class MyBaseOperator<T> extends BaseOperator implements  Serializable{
    public MyBaseOperator(){

    }
    public abstract DefaultInputPortSerializable<T> getInputPort();
    public abstract DefaultOutputPortSerializable getOutputPort();
    public  abstract DefaultInputPortSerializable getControlPort();
    public  abstract DefaultOutputPortSerializable<Boolean> getControlOut();


}
