package net.bitnine.agenspop;

import net.bitnine.agenspop.elastic.ElasticGraphService;
import net.bitnine.agenspop.graph.structure.AgensFactory;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@RunWith(SpringRunner.class)
@ActiveProfiles("local")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class Agenspop01ApplicationTests {

	Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${agens.api.base-path}")
	String basePath;		// => "api"

	@LocalServerPort()
	private int port;
	private URL base;

	@Autowired
	private TestRestTemplate template;
	@Autowired
	private GraphManager manager;
	@Autowired
	ElasticGraphService baseGraph;

	@Before
	public void setUp() throws Exception {
		this.base = new URL("http://localhost:" + port + basePath);
		logger.info("##### base url is {}", base.toString());
	}

	// **참고
	// https://www.baeldung.com/spring-boot-testresttemplate
	// https://www.baeldung.com/java-assert-string-not-empty
	// Null 값 체크 ==> assertThat( mc, is(nullValue()) );

	@Test
	public void contextLoads() {
		ResponseEntity<String> response;

		// graph
		response = template.getForEntity(base.toString()+"/graph/hello", String.class);
		assertThat("/graph is not loaded", response.getStatusCode(), equalTo(HttpStatus.OK));

		// admin
		response = template.getForEntity(base.toString()+"/admin/hello", String.class);
		assertThat("/admin is not loaded", response.getStatusCode(), equalTo(HttpStatus.OK));
	}

	////////////////////////////////////////////////

	@Test
	public void graphManagerLoad() {
		Set<String> names = manager.getGraphNames();
		assertTrue("graphs is empty", names.size() > 0);
	}

	@Test
	public void graphModernTest() {
		AgensGraph g = (AgensGraph) manager.getGraph("modern");
		if( g == null ) return;

		GraphTraversalSource t = g.traversal();

		List<Vertex> vertexList = t.V().next(100);
		assertTrue("vertices is empty", vertexList.size() > 0);

		List<Edge> edgeList = t.E().next(100);
		assertTrue("edges is empty", edgeList.size() > 0);

		assertTrue("V[modern_1] is not exist", t.V("modern_1").hasNext());

		vertexList = t.V("modern_5", "modern_4", "modern_3").next(100);
		assertTrue("vertices by ids list is not working", vertexList.size()==3);

		edgeList = t.V("modern_1").bothE().next(100);
		assertTrue("V.bothE() is not working", edgeList.size() > 0);

		Vertex v1 = t.V("modern_1").next();
		vertexList = t.V(v1).out().next(100);
		assertTrue("V.out() is not working", vertexList.size() > 0);

		vertexList = t.V().next(100);
		List<Object> valueList = t.V().values("name").next(100);
		assertTrue("V.values('name') is not working", vertexList.size() == valueList.size());

		vertexList = t.V().has("name","josh").next(100);
		assertTrue("V.has('name','josh') is not working", vertexList.size() > 0);

		vertexList = t.V().hasLabel("person").out("knows").next(100);
		assertTrue("V.hasLabel('person').out('knows') is not working", vertexList.size() > 0);

		vertexList = t.V().hasLabel("person").out("knows").where(__.values("age").is(P.lt(30))).next(100);
		assertTrue("...where(values(age).is(P.lt(30))) is not working", vertexList.size() > 0);
	}

	@Test
	public void graphCrudTest() {
		AgensGraph g = AgensFactory.createEmpty(baseGraph, "modern");
		assertThat( g, is(not(nullValue())) );
	}
}
