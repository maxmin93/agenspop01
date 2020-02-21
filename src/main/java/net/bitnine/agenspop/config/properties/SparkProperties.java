package net.bitnine.agenspop.config.properties;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotBlank;

@Getter
@Setter
@ConfigurationProperties(prefix = "agens.spark")
public class SparkProperties {

    @NotBlank
    private String appName;
    @NotBlank
    private String sparkHome;
    @NotBlank
    private String masterUri;

}
