package com.datatorrent.example.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.Partitioner;
import scala.Option;

import java.io.Serializable;

/**
 * Created by harsh on 12/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class ApexRDDOptionPartitioner extends Option<Partitioner> implements Serializable{
    public ApexRDDOptionPartitioner(){}
    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Partitioner get() {
        return new ApexRDDPartitioner();
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }

    @Override
    public boolean equals(Object that) {
        return false;
    }
}
