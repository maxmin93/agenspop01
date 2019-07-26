package net.bitnine.agenspop;

import net.bitnine.agenspop.config.properties.ElasticProperties;
import net.bitnine.agenspop.config.properties.ProductProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.ApplicationPidFileWriter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

@SpringBootApplication
@EnableConfigurationProperties({ ElasticProperties.class, ProductProperties.class })
public class Agenspop01Application {

	public static void main(String[] args) {

		SpringApplication application = new SpringApplication(Agenspop01Application.class);
		application.addListeners(new ApplicationPidFileWriter());	// pid file

		// application run
		ConfigurableApplicationContext ctx = application.run(args);
		ProductProperties productProperties = ctx.getBean(ProductProperties.class);

		// notify startup of server
		String hello_msg = productProperties.getHelloMsg();
		if( hello_msg != null ){
			System.out.println("\n================================================");
			System.out.println(" " + hello_msg);
			System.out.println("================================================\n");
		}
	}

}
