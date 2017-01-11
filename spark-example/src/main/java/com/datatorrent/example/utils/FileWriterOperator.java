package com.datatorrent.example.utils;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.exception.AlluxioException;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;

public class FileWriterOperator extends BaseOperator
{
    private BufferedWriter bw;
    private String absoluteFilePath;
    private FileSystem hdfs;
    //  private String absoluteFilePath = "hdfs://localhost:54310/tmp/spark-apex/output";
    public FileWriterOperator()
    {
    }
    public static void writeFileToAlluxio(String path,Object o) throws IOException, AlluxioException {
        alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
        AlluxioURI pathURI=new AlluxioURI(path);
        if(fs.exists(pathURI))
            fs.delete(pathURI);
        FileOutStream outStream = fs.createFile(pathURI);

        ObjectOutputStream oos = new ObjectOutputStream(outStream);
        oos.writeObject(o);
        oos.close();
        outStream.close();
        pathURI=new AlluxioURI("/_SUCCESS");
        outStream =fs.createFile(pathURI);
        DataOutputStream ds = new DataOutputStream(outStream);
        ds.write("Success File Created".getBytes());

    }
    public synchronized static void writeFileToHDFS(String path,Object o){

    }
    public synchronized  static void writeFileToLocal(String path,Object o) throws IOException {
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

                writeFileToAlluxio(absoluteFilePath, tuple);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (AlluxioException e) {
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
