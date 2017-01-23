package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by harsh on 22/1/17.
 */
@DefaultSerializer(JavaSerializer.class)
public class SplitRecordReader<T> extends MyBaseOperator<T> implements InputOperator,Serializable {
    public String path;

    public InputSplit splits[];
    public FileInputFormat fileInputFormat;
    public boolean shutApp=false;
    public String appName="";
    public RecordReader recordReader;
    public JobConf jobConf;
    public int minPartitions;
    public LongWritable longWritable;
    public Text text;
    private int operatorId;

    public SplitRecordReader(){}
    public final  DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
    public final  DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();

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
            recordReader = fileInputFormat.getRecordReader(splits[operatorId-1],jobConf,Reporter.NULL);
            if(recordReader.next(longWritable,text))
            {
                output.emit(text.toString());
            }
            else {
                sent=true;
            }
        }
        catch (Exception o){

        }
    }
    public InputSplit[] splitFileRecorder(String path, int minPartitions){
        Configuration conf = new Configuration(true);
        JobConf jobConf = new JobConf(conf);

        FileInputFormat fileInputFormat = new TextInputFormat();
        ((TextInputFormat)fileInputFormat).configure(jobConf);
        fileInputFormat.addInputPaths(jobConf,path);
        try {
            return  fileInputFormat.getSplits(jobConf,minPartitions);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void endWindow() {
        super.endWindow();
        if(shutApp) {
            shutApp = false;
            try {
                if(checkSucess("hdfs://localhost:54310/harsh/chi/success/Chi"+appName+"Success"))
                    throw new ShutdownException();
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
        operatorId = context.getId();
        try{
            Configuration conf = new Configuration();
            jobConf = new JobConf(conf);
            fileInputFormat = new TextInputFormat();
            ((TextInputFormat)fileInputFormat).configure(jobConf);
            text = new Text();
            longWritable = new LongWritable();
            splits=splitFileRecorder(path,minPartitions);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public boolean checkSucess(String path) throws IOException {
        Configuration conf = new Configuration();
        Path pt=new Path(path);
        FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
        if(hdfs.exists(pt))
        {
            //hdfs.delete(pt,false);
            return true;
        }
        else
            return false;

    }

    @Override
    public String toString() {
        return super.toString();
    }

}
