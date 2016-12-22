package com.datatorrent.example.utils;

import org.apache.spark.Partitioner;

/**
 * Created by harsh on 12/12/16.
 */
public class ApexRDDPartitioner extends Partitioner
{
    private int numParts = 1;

    public ApexRDDPartitioner(){

    }
    @Override
    public int getPartition(Object key) {
        return key.hashCode();
    }

    @Override
    public int numPartitions() {
        return numParts;
    }
}

