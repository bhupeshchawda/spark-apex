package com.datatorrent.example.utils;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

/**
 * Created by harsh on 5/1/17.
 */
public class ApexDoTask {

    public ApexDoTask(){}

    public static void launch(StreamingApplication app, String name, String libjars) throws Exception {
        Configuration conf = new Configuration(true);
//    conf.set("dt.loggers.level", "org.apache.*:DEBUG, com.datatorrent.*:DEBUG");
        conf.set("dt.dfsRootDirectory","/tmp");
        conf.set("fs.defaultFS", "hdfs://localhost:54310");
        conf.set("yarn.resourcemanager.address", "http://localhost:8032");
        conf.set("yarn.scheduler.maximum-allocation-mb","4096");
        conf.set("yarn.nodemanager.resource.memory-mb","4096");
        conf.addResource(new File("/home/harsh/apex-integration/spark-apex/spark-example/src/main/resources/data/mem.xml").toURI().toURL());
        //conf.addResource(new File(System.getProperty("$HOME/.dt/dt-site.xml")).toURI().toURL());

        if (libjars != null) {
            conf.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, libjars);
        }
        StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
        appLauncher.loadDependencies();
        StreamingAppFactory appFactory = new StreamingAppFactory(app, name);
        appLauncher.launchApp(appFactory);

    }

    public static void launch(StreamingApplication app, String name) throws Exception {
        launch(app, name, null);
    }
}
