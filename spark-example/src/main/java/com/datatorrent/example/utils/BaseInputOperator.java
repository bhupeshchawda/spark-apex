package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.example.MyBaseOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.DTFile;
import scala.tools.cmd.gen.AnyVals;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by harsh on 2/12/16.
 */
public abstract class BaseInputOperator extends MyBaseOperator implements InputOperator {
    public BaseInputOperator(){

    }

    public void emitTuples() {

    }
    public final transient DefaultOutputPortSerializable<String> output = new DefaultOutputPortSerializable<String>();
    public final transient DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        try{
            Path pt=new Path("hdfs://..");
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
