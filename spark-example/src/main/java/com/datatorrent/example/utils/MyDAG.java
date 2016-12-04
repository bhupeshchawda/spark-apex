package com.datatorrent.example.utils;

import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import java.io.Serializable;
import java.util.Stack;

/**
 * Created by anurag on 3/12/16.
 */
public class MyDAG extends LogicalPlan implements Serializable{
    public static String getLastOperatorName() {
        return lastOperatorName;
    }

    public static String getFirstOperatorName() {
        return firstOperatorName;
    }

    public static String lastOperatorName;
    public static String firstOperatorName;

    public Stack<String> stackName = new Stack<String>();

    @Override
    public <T extends Operator> T addOperator(String name, T operator) {
        stackName.push(name);
        lastOperatorName =name;
        firstOperatorName =stackName.firstElement();
        return super.addOperator(name,operator);
    }

    @Override
    public <T extends Operator> T addOperator(String name, Class<T> clazz) {
        return super.addOperator(name, clazz);
    }

    @Override
    public StreamMeta addStream(String id) {
        return super.addStream(id);
    }

    @Override
    public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>... sinks) {
//        stack.push((DefaultOutputPortSerializable) source);
        return super.addStream(id, source, sinks);
    }

    @Override
    public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1) {
        return super.addStream(id, source, sink1);
    }

    @Override
    public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2) {
        return super.addStream(id, source, sink1, sink2);
    }
}
