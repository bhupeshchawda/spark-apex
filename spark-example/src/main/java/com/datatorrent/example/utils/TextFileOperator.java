package com.datatorrent.example.utils;

import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class TextFileOperator extends BaseOperator implements InputOperator
{
  private long tuplesInWindow = 0;
  private String path;

  @Override
  public void beginWindow(long windowId)
  {
    tuplesInWindow = 0;
  }


  public void emitTuples() {

  }
}
