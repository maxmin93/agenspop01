package net.bitnine.agenspop.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotBlank;

// application.yml 파일에서 prefix에 대응되는 설정값들을 class 로 로딩하기
@Configuration
@ConfigurationProperties(prefix = "agens.elasticgraph")
public class ElasticProperties {

    @NotBlank
    private String host;        // = "localhost";
    @NotBlank
    private int port;           // = 9200;

    private String username;
    private String password;
    private int pageSize;       // = 2500;

    // ElasticProperties(){ }   // default constructor

    public String getHost() { return host; }
    public int getPort() { return port; }

    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public int getPageSize() { return pageSize; }

    public void setHost(String host) { this.host = host; }
    public void setPort(int port) { this.port = port; }
    public void setUsername(String username) { this.username = username; }
    public void setPassword(String password) { this.password = password; }
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }
}
