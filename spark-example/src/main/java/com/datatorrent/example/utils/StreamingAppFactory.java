package com.datatorrent.example.utils;

import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * Created by anurag on 5/1/17.
 */
public class StreamingAppFactory implements StramAppLauncher.AppFactory {
    GenericApplication app;
    private String name;

    public StreamingAppFactory(GenericApplication app, String name) {
        this.app = app;
        this.name = name;
    }

    @Override
    public LogicalPlan createApp(LogicalPlanConfiguration logicalPlanConfiguration) {
        LogicalPlan dag = new LogicalPlan();
        logicalPlanConfiguration.prepareDAG(dag,app,getName());
        return dag;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDisplayName() {
        return null;
    }

}
