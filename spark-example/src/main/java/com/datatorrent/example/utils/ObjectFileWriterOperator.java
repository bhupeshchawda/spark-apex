package com.datatorrent.example.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

/**
 * Created by harsh on 5/1/17.
 */
@DefaultSerializer(JavaSerializer.class)
public class ObjectFileWriterOperator<T> extends MyBaseOperator<T> implements Serializable{
    private BufferedWriter bw;
    private FileSystem hdfs;
    OutputStream os;
    public String absoluteFilePath = "hdfs://localhost:54310";
    public ObjectFileWriterOperator(){}
    public DefaultInputPortSerializable<HashMap> input =  new DefaultInputPortSerializable<HashMap>() {
        @Override
        public void process(HashMap tuple) {
            Configuration configuration = new Configuration();
            try {
                hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
                Path file = new Path(absoluteFilePath);
                if (hdfs.exists(file)) {
                    hdfs.delete(file, true);
                }
                os = hdfs.create(file);
            } catch (Exception e) {
                throw new RuntimeException();
            }
            try {
                try{
                    ObjectOutputStream oos=new ObjectOutputStream(os);
                    oos.writeObject(tuple);
                    oos.flush();
                    oos.close();
                    hdfs.close();
                }catch (Exception e){
                    ObjectOutputStream oos=new ObjectOutputStream(os);
                    oos.writeObject(tuple);
                    oos.flush();
                    oos.close();
                    hdfs.close();
                }
            } catch(Exception e) {
                throw new RuntimeException(e);
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
