package net.bitnine.agenspop;

import net.bitnine.agenspop.config.properties.ElasticProperties;
import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.graph.AgensGraphManager;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.ApplicationPidFileWriter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.util.List;
import java.util.stream.Collectors;

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
			StringBuilder sb = new StringBuilder();
			for(int i=hello_msg.length()+2; i>0; i--) sb.append("=");
			System.out.println("\n"+sb.toString());
			System.out.println(" " + hello_msg);
			System.out.println(sb.toString()+"\n");
		}

		AgensGraphManager graphManager = ctx.getBean(AgensGraphManager.class);
		if( graphManager != null ){
			graphManager.setDefaultGraph();
		}
	}

}
