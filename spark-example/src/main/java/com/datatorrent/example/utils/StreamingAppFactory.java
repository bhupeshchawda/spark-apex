package com.datatorrent.example.utils;

/**
 * Created by harsh on 5/1/17.
 */

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class StreamingAppFactory implements StramAppLauncher.AppFactory {

    private StreamingApplication app;
    private String name;

    public StreamingAppFactory(StreamingApplication app, String name) {
        this.app = app;
        this.name = name;
    }

    public LogicalPlan createApp(LogicalPlanConfiguration planConfig) {
        LogicalPlan dag = new LogicalPlan();
        planConfig.prepareDAG(dag, app, getName());
        return dag;
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return name;
    }
}
