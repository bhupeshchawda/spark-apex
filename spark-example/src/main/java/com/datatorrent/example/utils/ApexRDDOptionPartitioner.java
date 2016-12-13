package com.datatorrent.example.utils;

import org.apache.spark.Partitioner;
import scala.Option;

/**
 * Created by harsh on 12/12/16.
 */
public class ApexRDDOptionPartitioner extends Option<Partitioner>{
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
