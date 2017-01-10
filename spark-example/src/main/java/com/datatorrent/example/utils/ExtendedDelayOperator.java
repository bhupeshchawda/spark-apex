package com.datatorrent.example.utils;

import com.datatorrent.common.util.DefaultDelayOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by harsh on 10/1/17.
 */
@DefaultSerializer(JavaSerializer.class)
public  class ExtendedDelayOperator<T> extends DefaultDelayOperator<T> implements Serializable {
    public ExtendedDelayOperator(){}
}
