package com.datatorrent.example.utils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class FileWriterOperator extends BaseOperator
{
    private BufferedWriter bw;
    private FileSystem hdfs;
    OutputStream os;
    boolean shutDown= false;
    public String absoluteFilePath = "hdfs://localhost:54310";

    public FileWriterOperator()
    {
    }

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
        @Override
        public void process(Object tuple)
        {
            Configuration configuration = new Configuration();
            try {
                hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
                //            hdfs = FileSystem.getLocal(configuration);
//
                Path file = new Path(absoluteFilePath);
                if (hdfs.exists(file)) {
                    hdfs.delete(file, true);
                }
                os = hdfs.create(file);
                //bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(absoluteFilePath)));
            } catch (Exception e) {
                throw new RuntimeException();
            }
            try {
                try{
                    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
                    br.write(tuple.toString());
                    br.close();
                    hdfs.close();
                    shutDown=true;
                   // bw.write(tuple.toString());
                }catch (Exception e){
                    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
                    br.write(tuple.toString());
                    br.close();
                    hdfs.close();
                    e.printStackTrace();
                   // bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(absoluteFilePath)));
                    //bw.write(tuple.toString());
                }
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    };

    @Override
    public void endWindow() {
        super.endWindow();
        if(shutDown){
            Configuration configuration = new Configuration();
        try {
            hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);

            Path file = new Path("hdfs://localhost:54310/harsh/chi/success/ChiReduceSuccess");
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

    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath += absoluteFilePath;
    }
}
