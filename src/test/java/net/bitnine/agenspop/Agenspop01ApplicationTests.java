package net.bitnine.agenspop;

import net.bitnine.agenspop.elastic.ElasticGraphService;
import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;
import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import net.bitnine.agenspop.graph.structure.*;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
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
	private ElasticGraphService baseGraph;
	@Autowired
	private ElasticsearchTemplate elastic;

	String testGraphName = "newGraph";

	@Before
	public void setUp() throws Exception {
		this.base = new URL("http://localhost:" + port + basePath);
		logger.info("##### base url is {}", base.toString());

		if( baseGraph.removeDatasource(testGraphName) )
			System.out.println("  .. Remove data of ["+testGraphName+"]");
	}

	// **참고
	// https://www.baeldung.com/spring-boot-testresttemplate
	// https://www.baeldung.com/java-assert-string-not-empty
	// Null 값 체크 ==> assertThat( mc, is(nullValue()) );
	// https://m.blog.naver.com/PostView.nhn?blogId=simpolor&logNo=221327833587&proxyReferer=https%3A%2F%2Fwww.google.com%2F
	// https://www.lesstif.com/pages/viewpage.action?pageId=18219426#hamcrest%EB%A1%9C%EA%B0%80%EB%8F%85%EC%84%B1%EC%9E%88%EB%8A%94jUnitTestCase%EB%A7%8C%EB%93%A4%EA%B8%B0-Numbers

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
	public void managerGraphCreate() {
		AgensGraph g = AgensFactory.createEmpty(baseGraph, testGraphName);
		final Graph newGraph = manager.openGraph(testGraphName, (String gName) -> {
			return g;	// graph 바꿔치기
		});

		assertNotNull(g);
		assertThat(g, instanceOf(AgensGraph.class));
		// assertSame(g, newGraph);

		final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29, "country", "USA");
		final Vertex vadas = g.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27, "country", "USA");
		final Vertex lop = g.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
		final Vertex josh = g.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32, "country", "USA");
		final Vertex ripple = g.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java");
		final Vertex peter = g.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35, "country", "USA");
		marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5d);
		marko.addEdge("knows", josh, T.id, 8, "weight", 1.0d);
		marko.addEdge("created", lop, T.id, 9, "weight", 0.4d);
		josh.addEdge("created", ripple, T.id, 10, "weight", 1.0d);
		josh.addEdge("created", lop, T.id, 11, "weight", 0.4d);
		peter.addEdge("created", lop, T.id, 12, "weight", 0.2d);

		try {
			Thread.sleep(500); 	// Then do something meaningful...
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		long sizeV = baseGraph.countV(testGraphName);
		long sizeE = baseGraph.countE(testGraphName);

		assertThat("Some vertex insert fails", sizeV, is(6L));
		assertThat("Some edge insert fails", sizeE, is(6L));

		///////////////////////////////////

		GraphTraversalSource t = g.traversal();
		assertNotNull(t);

		List<Vertex> vertexList = t.V().next(100);
		assertTrue("vertices is empty", vertexList.size() > 0);

		List<Edge> edgeList = t.E().next(100);
		assertTrue("edges is empty", edgeList.size() > 0);

		Object vid = AgensIdManager.MIX_ID.convert(1,g);
		assertTrue("V["+vid.toString()+"] is not exist", t.V(vid).hasNext());

		Object[] vids = { AgensIdManager.MIX_ID.convert(3,g), AgensIdManager.MIX_ID.convert(4,g), AgensIdManager.MIX_ID.convert(5,g)};
		vertexList = t.V(vids).next(100);
		assertTrue("vertices by ids list is not working", vertexList.size()==3);

		edgeList = t.V(vid).bothE().next(100);
		assertTrue("V.bothE() is not working", edgeList.size() > 0);

		Vertex v1 = t.V(vid).next();
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

		///////////////////////////////////

		v1.remove();

		try {
			Thread.sleep(500); 	// Then do something meaningful...
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		sizeV = baseGraph.countV(testGraphName);
		sizeE = baseGraph.countE(testGraphName);

		assertThat("Removing "+v1.toString()+" fails", sizeV, is(5L));
		assertThat("Removing some edges fails", sizeE, is(3L));

		/////////////////////////////////////

		List<Edge> eList = t.E().next(100);
		for( Edge edge : eList ){
			System.out.println("  -- removing "+((AgensEdge)edge).toString());
			((AgensEdge)edge).remove();
		}
		List<Vertex> vList = t.V().next(100);
		for( Vertex vertex : vList ){
			System.out.println("  -- removing "+((AgensVertex)vertex).toString());
			((AgensVertex)vertex).remove();
		}

		sizeV = (int) baseGraph.countV(testGraphName);
		sizeE = (int) baseGraph.countE(testGraphName);

		System.out.println("Removing all vertices : "+testGraphName+".V() = "+sizeV);
		System.out.println("Removing all edges : "+testGraphName+".E() = "+sizeE);
	}

}
