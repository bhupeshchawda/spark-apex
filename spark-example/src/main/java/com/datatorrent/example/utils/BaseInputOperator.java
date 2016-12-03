package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.example.MyBaseOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;

/**
 * Created by harsh on 2/12/16.
 */
public class BaseInputOperator extends MyBaseOperator implements InputOperator,Serializable {
    public BaseInputOperator(){

    }
    public final transient DefaultOutputPortSerializable<String> output = new DefaultOutputPortSerializable<String>();
    public final transient DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();

    public DefaultInputPortSerializable<Object> getInputPort() {
        return null;
    }

    public DefaultOutputPortSerializable getOutputPort() {
        return output;
    }

    public DefaultOutputPortSerializable getControlPort() {
        return null;
    }

    public void emitTuples() {

    }


    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        try{
            Path pt=new Path("file:///home/anurag/spark-master/data/mllib/sample_libsvm_data.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

}
