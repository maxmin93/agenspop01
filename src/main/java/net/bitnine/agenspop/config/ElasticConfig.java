package net.bitnine.agenspop.config;

import net.bitnine.agenspop.elastic.repository.ArticleRepository;
import net.bitnine.agenspop.service.ArticleService;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@EnableElasticsearchRepositories(basePackages = "net.bitnine.agenspop.elastic.repository")
@ComponentScan(basePackages = { "net.bitnine.agenspop.elastic" })
public class ElasticConfig {

    @Autowired
    ElasticsearchOperations operations;
    @Autowired
    ArticleRepository repository;
    @Autowired
    ArticleService service;
    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;

    // ERROR : property 를 읽어오지 못하거나 타이밍이 안맞음 (null)
    //
//    @Value("${elasticsearch.host:localhost}")
//    @Value("${elasticsearch.port:9300}")
//    @Value("${elasticsearch.clustername")

//    public String host = "27.117.163.21";           //"localhost";
//    public String host = "tonyne.iptime.org";
//    public int port = 15620;                        //9300;
//    public int port = 9300;
//    private String clusterName = "es-bgmin";


    // ** NOTE: local 접속이 아니면 안됨 ==> High Level RestClient 로 차후 변경해야
    //          (spring-data-elasticsearch 에서 지원할 때 변경)
    public String host = "localhost";
    public int port = 9300;
    private String clusterName = "es-bitnine";

    public String getHost() {
        return host;
    }
    public int getPort() {
        return port;
    }
    public String getClusterName() {
        return clusterName;
    }

    @Bean
    public Client client(){
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .build();
        TransportClient client = null;
        try{
            System.out.println("** clusterName:'"+ clusterName +"', host:"+ host+", port:"+port);
            client = new PreBuiltTransportClient(settings)  //(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

// ** [ERROR] An exception occurred while running. null:
// ** InvocationTargetException: Invalid bean definition with name 'elasticsearchTemplate' defined in class path
// ** resource [org/springframework/boot/autoconfigure/data/elasticsearch/ElasticsearchDataAutoConfiguration.class]
//
//    @Bean
//    public ElasticsearchOperations elasticsearchTemplate() throws Exception {
//        return new ElasticsearchTemplate(client());
//    }

}
