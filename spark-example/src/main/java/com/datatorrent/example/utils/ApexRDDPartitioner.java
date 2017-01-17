package com.datatorrent.example.utils;

import com.datatorrent.api.Operator;
import org.apache.spark.Partitioner;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by harsh on 12/12/16.
 */
public class ApexRDDPartitioner extends Partitioner
{
    private int numParts = 2;

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
