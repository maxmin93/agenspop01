package net.bitnine.agenspop.service;

import net.bitnine.agenspop.util.AgensSparkHelper;
import net.bitnine.agenspop.dto.DataSetResult;
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.DataFrameReader;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

// @Service
public class AgensGraphxService {

/*
    private final JavaSparkContext jsc;
    private final SparkSession sql;

    @Autowired
    public AgensGraphxService( SparkContext sparkContext ) {
        this.jsc = new JavaSparkContext(sparkContext);
        this.sql = new SparkSession(sparkContext);
    }

    @PreDestroy
    public void close() throws Exception {
        if (jsc != null) {
            jsc.stop();
            // wait for jetty & spark to properly shutdown
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

    ///////////////////////////////////////

    public DataSetResult readIndex() {
        DataFrameReader reader = sql.read().format("org.elasticsearch.spark.sql");
        Dataset<Row> ds = reader.load("schools");

        DataSetResult result = AgensSparkHelper.getDataSetResult(ds);

        String fields = result.getColumnNames().stream().collect(Collectors.joining(","));
        int i = result.getRows().size();
        System.out.println("**readIndex: size="+i+" => "+fields);

        return result;
    }
 */

}
