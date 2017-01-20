package com.datatorrent.example.utils;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

import java.io.*;

//import org.apache.hadoop.fs.FileSystem;

public class FileWriterOperator extends BaseOperator
{
    private BufferedWriter bw;
    private String absoluteFilePath;
    public String successFilePath;
//    private FileSystem hdfs;
    //  private String absoluteFilePath = "hdfs://localhost:54310/tmp/spark-apex/output";
    public FileWriterOperator()
    {
    }
    public synchronized void createSuccessFile() throws IOException, AlluxioException {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI pathURI = new AlluxioURI("/user/anurag/spark-apex/_SUCCESS");
        FileOutStream outStream = fs.createFile(pathURI);
        DataOutputStream ds = new DataOutputStream(outStream);
        ds.write("Success File Created".getBytes());
        ds.close();
        outStream.close();

    }
    public synchronized  void writeFileToAlluxio(String path,Object o) throws IOException, AlluxioException {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI pathURI = new AlluxioURI("/user/anurag/spark-apex/"+path);
        if (fs.exists(pathURI))
            fs.delete(pathURI);
        FileOutStream outStream = fs.createFile(pathURI);
        ObjectOutputStream oos = new ObjectOutputStream(outStream);
        oos.writeObject(o);
        oos.close();
        outStream.close();
        createSuccessFile();
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
                System.out.println("Writing to file");
                if(!isSerialized) {
                    writeFileToAlluxio(absoluteFilePath, tuple);
                    isSerialized=true;
                }
            } catch (IOException | AlluxioException e) {
                e.printStackTrace();
            }
        }
    };

    @Override
    public void endWindow() {

    }
    public void setSuccessFilePath(String path){
        this.successFilePath=path;
    }
    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
    }
}
