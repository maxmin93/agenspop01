package net.bitnine.agenspop.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotBlank;

// application.yml 파일에서 prefix에 대응되는 설정값들을 class 로 로딩하기
@Getter @Setter
@ConfigurationProperties(prefix = "agens.elasticsearch")
public class ElasticProperties {

    @NotBlank
    private String host;        // = "localhost";
    @NotBlank
    private int port;           // = 9200;

    private String username;
    private String password;
    private int pageSize;       // = 2500;

    @NotBlank
    private String vertexIndex;
    @NotBlank
    private String edgeIndex;

}
