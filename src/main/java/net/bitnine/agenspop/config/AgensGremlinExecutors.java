package net.bitnine.agenspop.config;

import org.apache.tinkerpop.gremlin.server.util.ThreadFactoryUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

@Configuration
public class AgensGremlinExecutors {

    @Primary
    @Bean("gremlinExecutor")
    public ExecutorService getGremlinExecutor() {
        ThreadFactory threadFactory = ThreadFactoryUtil.create("exec-%d");
        return Executors.newFixedThreadPool(5, threadFactory);
    }

    @Bean("scheduledExecutor")
    public ScheduledExecutorService getScheduledExecutor() {
        ThreadFactory threadFactory = ThreadFactoryUtil.create("worker-%d");
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

}
