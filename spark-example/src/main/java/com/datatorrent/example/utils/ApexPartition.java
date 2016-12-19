package com.datatorrent.example.utils;

import org.apache.spark.Partition;

import java.io.Serializable;

/**
 * Created by anurag on 14/12/16.
 */
public abstract class ApexPartition implements Partition,Serializable {
    @Override
    public int index() {
        return 0;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other);
    }
}
