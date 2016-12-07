package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.example.MyBaseOperator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;

/**
 * Created by harsh on 2/12/16.
 */

public class BaseInputOperator extends MyBaseOperator implements InputOperator,Serializable {
    private BufferedReader br;

    public BaseInputOperator(){

    }

    public DefaultOutputPortSerializable<Integer> getCountOutputPort() {
        return null;
    }

    public final transient DefaultOutputPortSerializable<String> output = new DefaultOutputPortSerializable<String>();
    public final transient DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();

    public DefaultInputPortSerializable<Object> getInputPort() {
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
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
        if(sent){
            controlOut.emit(true);
        }
    }

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        try{
//            Path pt=new Path("file:///home/anurag/spark-apex/spark-example/src/main/resources/data/sample_libsvm_data.txt");
            FileInputStream fs = new FileInputStream("/home/anurag/spark-apex/spark-example/src/main/resources/data/sample_libsvm_data.txt");
            br=new BufferedReader(new InputStreamReader(fs));


        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

}
