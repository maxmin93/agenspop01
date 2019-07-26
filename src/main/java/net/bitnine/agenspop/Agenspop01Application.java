package net.bitnine.agenspop;

import net.bitnine.agenspop.config.properties.ElasticProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ElasticProperties.class)
public class Agenspop01Application {

	public static void main(String[] args) {

		SpringApplication.run(Agenspop01Application.class, args);
	}

}
