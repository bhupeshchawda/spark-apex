package com.datatorrent.example.utils;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

/**
 * Created by harsh on 12/12/16.
 */
public class ApexRDDStreamCodec extends KryoSerializableStreamCodec {
    @Override
    public int getPartition(Object o) {
        return o.hashCode();
    }
}
