package com.datatorrent.example.utils;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.exception.AlluxioException;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.example.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;

/**
 * Created by harsh on 2/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class BaseInputOperator<T> extends MyBaseOperator<T> implements InputOperator,Serializable {
    private transient BufferedReader br;
    public String path;

    public BaseInputOperator(){

    }



    public   DefaultOutputPortSerializable<String> output = new DefaultOutputPortSerializable<>();
    public   DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();

    @Override
    public DefaultInputPortSerializable<T> getInputPort() {return null;}
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
            String line = br.readLine();
            if (line != null) {
                log.info("Reading lines");
                output.emit(line);

            }
            else {

                sent=true;
            }
        }
        catch (Exception o){

        }
    }
    public  boolean successFileExists() throws IOException, AlluxioException {
        alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
        AlluxioURI pathURI=new AlluxioURI("/user/anurag/spark-apex/_SUCCESS");
        return fs.exists(pathURI);

    }
    @Override
    public void beginWindow(long windowId) {
        if(sent){
            try {
                if(successFileExists()) {
                     deleteSUCCESSFile();
                     log.info("Killing Application");
//                    throw new ShutdownException();
                }
            } catch (IOException | AlluxioException e) {
                e.printStackTrace();
            }


            controlOut.emit(true);
        }
    }
    Logger log = LoggerFactory.getLogger(BaseInputOperator.class);
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        try{
//            Path pt=new Path("file:///home/anurag/spark-apex/spark-example/src/main/resources/data/sample_libsvm_data.txt");
//            Configuration conf = new Configuration();
//            Path pt=new Path(path);
//            FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
            alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
            AlluxioURI pathURI=new AlluxioURI(path);
            FileInStream inStream = fs.openFile(pathURI);
            br=new BufferedReader(new InputStreamReader(inStream));



        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }
    public synchronized void deleteSUCCESSFile() {
        try {
            alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
            AlluxioURI pathURI=new AlluxioURI("/user/anurag/spark-apex/_SUCCESS");
            if(fs.exists(pathURI)) fs.delete(pathURI);

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
        }

    }
}
