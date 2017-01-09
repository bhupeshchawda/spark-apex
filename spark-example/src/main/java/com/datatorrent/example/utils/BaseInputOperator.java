package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.example.ApexRDD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;

/**
 * Created by harsh on 2/12/16.
 */

public class BaseInputOperator<T> extends MyBaseOperator<T> implements InputOperator,Serializable {
    private BufferedReader br;
    public String path;
    public boolean shutApp=false;

    public BaseInputOperator(){

    }
    public final transient DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
    public final transient DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();

    public DefaultInputPortSerializable<T> getInputPort() {
        return null;
    }

    public DefaultOutputPortSerializable getOutputPort() {
        return output;
    }

    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return controlOut;
    }
    public boolean sent=false;
    public void emitTuples() {
        try {
            String line = br.readLine();

            if (line != null) {
                    output.emit(line);
            }
            else {
                sent=true;
            }
        }
        catch (Exception o){

        }
    }

    @Override
    public void endWindow() {
        super.endWindow();
        if(shutApp) {
            shutApp=false;
            try {
                if (checkSucess("hdfs://localhost:54310/harsh/chi/success/ChiReduceSuccess")) {
                    throw new ShutdownException();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
        if(sent) {
            controlOut.emit(true);
            shutApp=true;
        }
    }

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        try{
            Configuration conf = new Configuration();
            Path pt=new Path("hdfs://localhost:54310/harsh/chi/sample_libsvm_data.txt");
            FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
            br=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public boolean checkSucess(String path) throws IOException {
        Configuration conf = new Configuration();
        Path pt=new Path("hdfs://localhost:54310/harsh/chi/success/ChiReduceSuccess");
        FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
        if(hdfs.exists(pt))
            return true;
        else
            return false;

    }

    @Override
    public String toString() {
        return super.toString();
    }

}
