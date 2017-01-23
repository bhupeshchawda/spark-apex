package com.datatorrent.example.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.mapred.FileSplit;

import java.io.Serializable;

/**
 * Created by harsh on 23/1/17.
 */
@DefaultSerializer(JavaSerializer.class)
public class SerializedInputSplit extends FileSplit implements Serializable {
    public SerializedInputSplit(){}
}
