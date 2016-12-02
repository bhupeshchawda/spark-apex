package com.datatorrent.example;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.example.utils.DefaultInputPortSerializable;
import com.datatorrent.example.utils.DefaultOutputPortSerializable;

/**
 * Created by harsh on 2/12/16.
 */
public abstract class MyBaseOperator extends BaseOperator {
    public MyBaseOperator(){}



    public abstract DefaultInputPortSerializable<Object> getInputPort();
    public abstract DefaultOutputPortSerializable getOutputPort();
    public  abstract DefaultOutputPortSerializable getControlPort();


}
