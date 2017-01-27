package com.datatorrent.example.utils;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by krushika on 20/1/17.
 */
public class AlluxioOperations {
    public void AlluxioReader(String path) throws IOException, AlluxioException {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI alluxioURI = new AlluxioURI(path);
        FileInStream fis = fs.openFile(alluxioURI);
        DataInputStream dis = new DataInputStream(fis);
        dis.readChar();
        dis.close();
        fis.close();
    }

    public void AlluxioWriter(String path) throws IOException, AlluxioException {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI alluxioURI = new AlluxioURI(path);
        FileOutStream fos = fs.createFile(alluxioURI);
        DataOutputStream dos = new DataOutputStream(fos);
        dos.writeChars(String.valueOf(this));
        dos.close();
        fos.close();
    }

    public void AlluxioSaveRDD(){

    }

    public void AlluxioWriteObject(String path, Object o) throws IOException, AlluxioException {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI alluxioURI = new AlluxioURI(path);
        FileOutStream fos = fs.createFile(alluxioURI);
        ObjectOutputStream os = new ObjectOutputStream(fos);
        os.writeObject(o);
        os.close();
        fos.close();

    }
}
