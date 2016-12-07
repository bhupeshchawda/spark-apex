package com.datatorrent.example.utils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

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
//            hdfs = FileSystem.getLocal(configuration);
//
//            Path file = new Path(absoluteFilePath);
//            if (hdfs.exists(file)) {
////                hdfs.delete(file, true);
//            }
//            OutputStream os = hdfs.create(file);
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(absoluteFilePath)));
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
        @Override
        public void process(Object tuple)
        {
            try {
                try{
                    bw.write(tuple.toString());
                }catch (Exception e){
                    bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(absoluteFilePath)));
                    bw.write(tuple.toString());
                }
                bw.close();
//                hdfs.close();
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    };

    @Override
    public void endWindow() {
        super.endWindow();

    }

    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
    }
}
