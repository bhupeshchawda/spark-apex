package com.datatorrent.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by harsh on 2/12/16.
 */
public class MyBaseOperator extends BaseOperator {
    public MyBaseOperator(){}
    public DefaultInputPort inputPort= new DefaultInputPort() {
        @Override
        public void process(Object tuple) {
                  outputPort.emit(tuple);
        }
    };
    public DefaultOutputPort outputPort= new DefaultOutputPort();


    public DefaultInputPort getInputPort() {
        return inputPort;
    }
    public DefaultOutputPort getOutputPort(){
        return outputPort;
    }
    public void setInputPort(DefaultInputPort inputPort) {
        this.inputPort = inputPort;
    }

}
