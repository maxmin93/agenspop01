package net.bitnine.agenspop.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import net.bitnine.agenspop.util.AgensJacksonModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

@Configuration
public class AgensJacksonConfig {

//    @Bean("objectMapper")
//    public ObjectMapper objectMapper() {
//        return Jackson2ObjectMapperBuilder.json()
//                .featuresToDisable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
//                .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
//                .modules(new AgensJacksonModule())
//                .build();
//    }

}
