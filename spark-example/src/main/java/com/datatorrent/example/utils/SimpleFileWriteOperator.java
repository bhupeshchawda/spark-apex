package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.*;

/**
 * Created by harsh on 28/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class SimpleFileWriteOperator<T> extends MyBaseOperator<T> implements Serializable {
    public static BufferedWriter bw;
    public static String absoluteFilePath;
    public SimpleFileWriteOperator(){}

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(absoluteFilePath)));
            bw.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            try {
                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(absoluteFilePath,true)));
                bw.append(tuple.toString());
                bw.newLine();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    };

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
    }

    @Override
    public void endWindow() {
        super.endWindow();
    }

    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
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
