package com.datatorrent.example.utils;

import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import java.io.Serializable;
import java.util.Stack;

/**
 * Created by anurag on 3/12/16.
 */

public class MyDAG extends LogicalPlan implements Serializable{
    public  String getLastOperatorName() {
        return lastOperatorName;
    }

    public  String getFirstOperatorName() {
        return firstOperatorName;
    }

    public  String lastOperatorName;
    public  String firstOperatorName;

    public Stack<String> stackName = new Stack<String>();

    @Override
    public <T extends Operator> T addOperator(String name, T operator) {
        stackName.push(name);
        lastOperatorName =name;
        firstOperatorName =stackName.firstElement();
        return super.addOperator(name,operator);
    }

}
