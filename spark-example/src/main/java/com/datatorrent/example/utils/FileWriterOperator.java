package com.datatorrent.example.utils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class FileWriterOperator extends BaseOperator
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
    public synchronized  static void writeToFile(String path,Object o) throws IOException {
        FileOutputStream fos = new FileOutputStream(path);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(o);
        oos.close();

        FileOutputStream fileWriter = new FileOutputStream("/tmp/spark-apex/_SUCCESS");
        fileWriter.write("Writing Data to file".getBytes());
        fileWriter.close();

    }
    @Override
    public void beginWindow(long windowId) {

    }

    @Override
    public void setup(OperatorContext context)
    {
        isSerialized =false;
//        try {
//            kryo = new Kryo();
//            output = new Output(new FileOutputStream(absoluteFilePath));
//        } catch (Exception e) {
//            throw new RuntimeException();
//        }
    }
//    Logger log = LoggerFactory.getLogger(FileWriterOperator.class);

    private static boolean isSerialized;
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
        @Override
        public void process(Object tuple)
        {

            try {

                writeToFile(absoluteFilePath, tuple);
            } catch (IOException e) {
                throw new RuntimeException(e);
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
