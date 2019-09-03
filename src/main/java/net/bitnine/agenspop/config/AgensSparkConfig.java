package net.bitnine.agenspop.config;

import net.bitnine.agenspop.config.properties.SparkProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = { "net.bitnine.agenspop.service" })
public class AgensSparkConfig {

    private String appName;
    private String sparkHome;
    private String masterUri;

    @Autowired
    public AgensSparkConfig(SparkProperties properties){
        this.appName = properties.getAppName();
        this.sparkHome = properties.getSparkHome();
        this.masterUri = properties.getMasterUri();
    }

    // @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setSparkHome(sparkHome)
                .setMaster(masterUri)
                .set("spark.executor.memory", "2g")
                .set("spark.driver.memory", "2g")
                // es 접속정보
                // .set("es.nodes", "27.117.163.21")
                // .set("es.port", "15619")
                // .set("es.mapping.id", "_id")
                // .set("es.nodes.wan.only", "true")
                // .set("es.write.operation", "upsert")
                // .set("es.index.read.missing.as.empty", "true")
                ;
        return sparkConf;
    }

    @Bean       // auto closable
    public SparkContext sparkContext() {
        SparkContext sc = new SparkContext(sparkConf());
        sc.setLogLevel("ERROR");        // INFO
        return sc;
    }

}
