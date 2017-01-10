package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by harsh on 28/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class SimpleFileWriteOperator<T> extends MyBaseOperator<T> implements Serializable {
    public static BufferedWriter bw;
    public SimpleFileWriteOperator(){}
    Configuration configuration;
    OutputStream os;
    public String appName="";
    boolean closeStreams=false,shutDown=false;
    private FileSystem hdfs;
    public String absoluteFilePath = "hdfs://localhost:54310";

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        try {
            configuration = new Configuration();
            hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
            Path file = new Path(absoluteFilePath);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            os = hdfs.create(file);
            bw = new BufferedWriter(new OutputStreamWriter(os,"UTF-8"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            closeStreams = false;
            try {
                bw.append(tuple.toString());
                bw.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    };

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
        closeStreams = true;
        if(shutDown)
        {
            Configuration configuration = new Configuration();
            try {
                hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);

                Path file = new Path("hdfs://localhost:54310/harsh/chi/success/Chi"+appName+"Success");
                if (hdfs.exists(file)) {
                    hdfs.delete(file, true);
                }
                os = hdfs.create(file);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void endWindow() {
        super.endWindow();
        if(closeStreams){
            try {
                bw.close();
                hdfs.close();
                shutDown=true;
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath += absoluteFilePath;
    }
    @Override
    public DefaultInputPortSerializable<T> getInputPort() {
        return this.input;
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
