package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by anurag on 12/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class CollectOperator<T> extends BaseOperator implements Serializable {
    FileWriter  fw;
    BufferedWriter bw;
    PrintWriter out;
    Logger log = LoggerFactory.getLogger(CollectOperator.class);
    public CollectOperator(){}
    public  static ArrayList<Object> t = new ArrayList<>();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            try {
                t.add(tuple);
                count++;
            } catch (Exception e) {
                System.out.println("The ERROR has OCCURED");
                e.printStackTrace();
            }
        }
    };
    int count=0;
    @Override
    public void setup(Context.OperatorContext context) {
        try {
            t=new ArrayList<>();
            fw= new FileWriter("/tmp/collectedData",true);
            bw= new BufferedWriter(fw);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
