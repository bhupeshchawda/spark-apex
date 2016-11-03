package com.datatorrent.example.utils;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

import com.datatorrent.api.DefaultOutputPort;

public class FileReaderOperator extends LineByLineFileInputOperator
{
  private boolean sentControl = false;

  public final transient DefaultOutputPort<Boolean> controlOut = new DefaultOutputPort<>();

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
