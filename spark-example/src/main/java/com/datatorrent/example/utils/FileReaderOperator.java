package com.datatorrent.example.utils;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

public class FileReaderOperator extends LineByLineFileInputOperator
{
    private boolean sentControl = false;

    public final  DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();

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