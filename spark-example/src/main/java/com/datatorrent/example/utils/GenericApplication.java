package com.datatorrent.example.utils;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;

public class GenericApplication implements StreamingApplication
{
  private LogicalPlan dag;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    for (OperatorMeta o : this.dag.getAllOperators()) {
      dag.addOperator(o.getName(), o.getOperator());
    }
    for (StreamMeta s : this.dag.getAllStreams()) {
      for (InputPortMeta i : s.getSinks()) {
        Operator.OutputPort<Object> op = (OutputPort<Object>) s.getSource().getPortObject();
        Operator.InputPort<Object> ip = (InputPort<Object>) i.getPortObject();
        dag.addStream(s.getName(), op, ip);
        dag.setInputPortAttribute(s.getSinks().get(0).getPortObject(), Context.PortContext.STREAM_CODEC, 
            new JavaSerializationStreamCodec());
      }
    }
  }

  public void setDag(LogicalPlan dag)
  {
    this.dag = dag;
  }
}
