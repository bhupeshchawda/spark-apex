package com.datatorrent.example.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.*;
import java.util.HashMap;

/**
 * Created by harsh on 5/1/17.
 */
@DefaultSerializer(JavaSerializer.class)
public class ObjectFileWriterOperator<T> extends MyBaseOperator<T> implements Serializable{
    public ObjectFileWriterOperator(){}
    public static String absoluteFilePath;
    public DefaultInputPortSerializable<HashMap> input =  new DefaultInputPortSerializable<HashMap>() {
        @Override
        public void process(HashMap tuple) {
            File fileOne=new File(absoluteFilePath);
            FileOutputStream fos= null;
            try {
                fos = new FileOutputStream(fileOne);
                ObjectOutputStream oos=new ObjectOutputStream(fos);
                oos.writeObject(tuple);
                oos.flush();
                oos.close();
                fos.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }




        }
    };
    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
    }
    @Override
    public DefaultInputPortSerializable<T> getInputPort() {
        return (DefaultInputPortSerializable<T>) input;
    }

    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return null;
    }

    @Override
    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}
