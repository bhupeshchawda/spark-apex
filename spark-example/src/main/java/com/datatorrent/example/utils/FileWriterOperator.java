package com.datatorrent.example.utils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Serializable;

public class FileWriterOperator extends BaseOperator implements Serializable
{
    private BufferedWriter bw;
    private String absoluteFilePath;
    private FileSystem hdfs;
    private Kryo kryo;
    private Output output;
//  private String absoluteFilePath = "hdfs://localhost:54310/tmp/spark-apex/output";
    public FileWriterOperator()
    {
    }
    public static void writeToFile(String path,Object o) throws FileNotFoundException {
        Kryo kryo = new Kryo();
        Output output = new Output(new FileOutputStream(path));
        kryo.writeClassAndObject(output, o);
        output.close();
    }
    @Override
    public void beginWindow(long windowId) {

    }

    @Override
    public void setup(OperatorContext context)
    {
        try {
            kryo = new Kryo();
            output = new Output(new FileOutputStream(absoluteFilePath));
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
//    Logger log = LoggerFactory.getLogger(FileWriterOperator.class);

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
        @Override
        public void process(Object tuple)
        {

            try {
                writeToFile(absoluteFilePath,tuple);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    };

    @Override
    public void endWindow() {

    }

    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
    }
}
