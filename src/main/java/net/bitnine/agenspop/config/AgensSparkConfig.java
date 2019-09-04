package net.bitnine.agenspop.config;

import net.bitnine.agenspop.config.properties.ElasticProperties;
import net.bitnine.agenspop.config.properties.SparkProperties;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.annotation.Configuration;

//@Configuration
//@ComponentScan(basePackages = { "net.bitnine.agenspop.service" })
public class AgensSparkConfig {

    private String appName;
    private String sparkHome;
    private String masterUri;
    private String esHost;
    private String esPort;

    //@Autowired
    public AgensSparkConfig(SparkProperties sparkProperties, ElasticProperties elasticProperties){
        this.appName = sparkProperties.getAppName();
        this.sparkHome = sparkProperties.getSparkHome();
        this.masterUri = sparkProperties.getMasterUri();
        this.esHost = elasticProperties.getHost();
        this.esPort = String.valueOf(elasticProperties.getPort());
    }
/*
    // @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setSparkHome(sparkHome)
                .setMaster(masterUri)
                .set("spark.executor.memory", "2g")
                .set("spark.driver.memory", "2g")
                // es 접속정보
                .set("es.nodes", esHost)
                .set("es.port", esPort)
                // .set("es.mapping.id", "_id")
                .set("es.nodes.wan.only", "true")
                .set("es.write.operation", "upsert")
                // .set("es.index.auto.create", "true")
                .set("es.index.read.missing.as.empty", "true")
                .set("es.http.timeout","10m")
                .set("es.scroll.size","5000")
                ;
        return sparkConf;
    }

    @Bean       // auto closable
    public SparkContext sparkContext() {
        SparkContext sc = new SparkContext(sparkConf());
        sc.setLogLevel("ERROR");        // INFO
        return sc;
    }
*/
}
