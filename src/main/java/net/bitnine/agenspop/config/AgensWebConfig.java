package net.bitnine.agenspop.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class AgensWebConfig implements WebMvcConfigurer {

    @Value("${agens.api.base-path}")
    private String basePath;

    @Override
    public void addCorsMappings(CorsRegistry corsRegistry) {
        corsRegistry.addMapping(basePath+"/**");

        // **참고 https://www.baeldung.com/spring-value-annotation
//		registry.addMapping("/api/**")
//		.allowedOrigins("http://domain2.com")
//		.allowedMethods("PUT", "DELETE")
//		.allowedHeaders("header1", "header2", "header3")
//		.exposedHeaders("header1", "header2")
//		.allowCredentials(false).maxAge(3600);
    }


}
