package net.bitnine.agenspop.config;

import net.bitnine.agenspop.config.properties.ElasticProperties;
import net.bitnine.agenspop.elastic.repository.ArticleRepository;

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
public class AgensElasticConfig {

    // ** NOTE: local 접속이 아니면 안됨
    //      ==> High Level RestClient 로 차후 변경해야
    //          (spring-data-elasticsearch v3.2 에서 지원할 때 변경)
    private final String host;
    private final int port;
    private final String clusterName;

    @Autowired
    AgensElasticConfig(ElasticProperties elasticProperties){
        this.host = elasticProperties.getHost();
        this.port = elasticProperties.getPort();
        this.clusterName = elasticProperties.getClusterName();
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


}
