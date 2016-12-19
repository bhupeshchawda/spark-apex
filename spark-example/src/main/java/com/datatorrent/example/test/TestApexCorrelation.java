package com.datatorrent.example.test;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;

import java.io.Serializable;

/**
 * Created by harsh on 17/12/16.
 */
public class TestApexCorrelation implements Serializable{
    public TestApexCorrelation(){}

    public TestApexCorrelation(ApexContext sc)
    {
    /*
        ApexRDD seriesX = sc.parallelize(
                , 1, null);  // a series

        // must have the same number of partitions and cardinality as seriesX
        ApexRDD seriesY = sc.parallelize(
                Arrays.asList(11.0, 22.0, 33.0, 33.0, 555.0));
*/

    }
    public static void main(String args[]){
        ApexContext sc  = new ApexContext(new ApexConf().setMaster("local").setAppName("ApexApp"));
        TestApexCorrelation t = new TestApexCorrelation(sc);
    }
}
