package com.datatorrent.example.utils;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class FileWriterOperator extends BaseOperator
{
    private BufferedWriter bw;
    private String absoluteFilePath;
    private FileSystem hdfs;
//  private String absoluteFilePath = "hdfs://localhost:54310/tmp/spark-apex/output";

    public FileWriterOperator()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
        Configuration configuration = new Configuration();
        try {
//      hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
            hdfs = FileSystem.getLocal(configuration);
            Path file = new Path(absoluteFilePath);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            OutputStream os = hdfs.create(file);
            bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public final transient DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
        @Override
        public void process(Object tuple)
        {
            try {
                bw.write(tuple.toString());
                bw.close();
                hdfs.close();
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    };

    public String getAbsoluteFilePath()
    {
        return absoluteFilePath;
    }

    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
    }
}
