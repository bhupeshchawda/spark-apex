package com.datatorrent.example.utils;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by anurag on 2/12/16.
 */

public class FileReadOperator extends AbstractFileInputOperator<String>  {
    private BufferedReader br;
    public transient DefaultOutputPortSerializable output=new DefaultOutputPortSerializable();
    @Override
    protected InputStream openFile(Path path) throws IOException {
        InputStream is = super.openFile(path);
        br= new BufferedReader(new InputStreamReader(is));
        return is;
    }

    protected String readEntity() throws IOException {
        String s =  br.readLine();
        return s;
    }

    protected void emit(String s) {
        output.emit(s);
    }
    private boolean sentControl = false;

    public DefaultOutputPortSerializable getOutput() {
        return output;
    }

    public void setOutput(DefaultOutputPortSerializable output) {
        this.output = output;
    }

    public final transient DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();

    @Override
    public void beginWindow(long windowId)
    {
        super.beginWindow(windowId);

        if (pendingFiles.isEmpty() && !sentControl) {
            controlOut.emit(true);
            sentControl = true;
        }
    }
}
