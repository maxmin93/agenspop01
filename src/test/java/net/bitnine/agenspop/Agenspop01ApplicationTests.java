package net.bitnine.agenspop;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.bitnine.agenspop.elasticgraph.ElasticGraphAPI;
import net.bitnine.agenspop.graph.AgensGraphManager;
import net.bitnine.agenspop.graph.structure.*;
import net.bitnine.agenspop.service.AgensGremlinService;
import net.bitnine.agenspop.util.AgensJacksonModule;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.After;
import org.junit.Before;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

@RunWith(SpringRunner.class)
@ActiveProfiles("local")
@SpringBootTest(
		webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
		properties = "classpath:application.yml"
)
public class Agenspop01ApplicationTests {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${agens.api.base-path}")
	String basePath;		// => "/api"

	@LocalServerPort
	private int port;
	// private URL base;

	@Autowired
	private TestRestTemplate rest;
	@Autowired
	private AgensGremlinService gremlin;
	@Autowired
	private AgensGraphManager manager;
	@Autowired
	private ElasticGraphAPI api;
	@Autowired
	private RestHighLevelClient client;

	AgensGraph g;
	String gName = "test";
	String datasource = "modern";

	/*
	///////////////////////////////////////
	// called for every test

	@Before
	public void setUp() throws Exception {
		this.g = (AgensGraph) manager.openGraph(gName);
	}

	@After
	public void clear() throws Exception {
		Graph removed = manager.removeGraph(gName);
		System.out.println("graph["+gName+"] removed: "+removed.toString());
	}
	///////////////////////////////////////
	*/

	@Test
	public void contexLoads() throws Exception {
		assertThat( client, is(notNullValue()));
		assertThat( gremlin, is(notNullValue()));
		assertThat( api, is(notNullValue()));
		assertThat( manager, is(notNullValue()));
		assertThat( rest, is(notNullValue()));
	}

	@Test
	public void restShouldReturnDefaultMessage() throws Exception {
		String prefixUrl = "http://localhost:" + port + basePath;

		assertThat( this.rest.getForObject(prefixUrl+"/graph/hello",
				String.class), is(containsString("Hello, graph!")));
		assertThat( this.rest.getForObject(prefixUrl+"/admin/hello",
				String.class), is(containsString("Hello, admin!")));
		assertThat( this.rest.getForObject(prefixUrl+"/search/hello",
				String.class), is(containsString("Hello, search!")));
		assertThat( this.rest.getForObject(prefixUrl+"/spark/hello",
				String.class), is(containsString("Hello, spark!")));
	}

	@Test
	public void graphManagerLoad() {
		Set<String> names = manager.getGraphNames();
		assertTrue("graphs is empty", names.size() > 0);
	}

	@Test
	public void apiAdminLabels() {
		Map<String, Map<String,Long>> labels = manager.getGraphLabels(datasource);
		assertEquals( "labels of V = "+labels.get("V").size(), 2, labels.get("V").size());
		assertEquals( "labels of E = "+labels.get("E").size(), 2, labels.get("E").size());
	}

	@Test
	public void apiAdminKeys() {
		String label = "person";
		Map<String,Long> keys = manager.getGraphKeys(datasource, label);
		assertEquals( "keys of '"+label+"' = "+keys.keySet().size(), 3, keys.keySet().size());
	}

	private Stream<Object> runGremlin(String script){
		Stream<Object> stream = Stream.empty();
		try {
			CompletableFuture<?> future = gremlin.runGremlin(script);
			CompletableFuture.allOf(future).join();
			stream = (Stream<Object>) future.get();
		}catch (Exception ex){
			System.out.println("** ERROR: runGremlin ==> " + ex.getMessage());
		}
		return stream;
	}

	@Test
	public void apiGraphGremlinGrammar01(){
		String script;
		long result;

		script = "modern_g.V()";
		result = runGremlin(script).count();
		assertEquals( script+" => "+result, 6, result);

		script = "modern_g.E()";
		result = runGremlin(script).count();
		assertEquals( script+" => "+result, 6, result);
	}

	@Test
	public void apiGraphGremlinGrammar02(){
		String script;
		long result;

		script = "modern_g.V().has(\"age\",gt(30))";
		result = runGremlin(script).count();
		assertEquals( script+" => "+result, 2, result);

		script = "modern_g.V().has(\"name\",\"marko\").out().out()";
		result = runGremlin(script).count();
		assertEquals( script+" => "+result, 2, result);

		// 2 = modern_g.V().groupCount().by(T.label)
		script = "modern_g.V().groupCount().by(T.label)";
		Map<String,Long> grp1 = (Map<String,Long>)runGremlin(script).collect(Collectors.toList()).get(0);
		assertEquals( script+" => "+grp1.size(), 2, grp1.size());

		// 3 = modern_g.V().hasLabel("person").properties().key().groupCount()
		script = "modern_g.V().hasLabel(\"person\").properties().key().groupCount()";
		Map<String,Long> grp2 = (Map<String,Long>)runGremlin(script).collect(Collectors.toList()).get(0);
		assertEquals( script+" => "+grp2.size(), 3, grp2.size());

		// 2 = modern_g.E().project("self","inL","outL").by(__.label()).by(__.inV().label()).by(__.outV().label()).groupCount()
		script = "modern_g.E().project(\"self\",\"inL\",\"outL\").by(__.label()).by(__.inV().label()).by(__.outV().label()).groupCount()";
		Map<String,Long> grp3 = (Map<String,Long>)runGremlin(script).collect(Collectors.toList()).get(0);
		assertEquals( script+" => "+grp3.size(), 2, grp3.size());
	}

	private Stream<Object> runCypher(String script){
		Stream<Object> stream = Stream.empty();
		try {
			CompletableFuture<?> future = gremlin.runCypher(script, datasource);
			CompletableFuture.allOf(future).join();
			stream = (Stream<Object>) future.get();
		}catch (Exception ex){
			System.out.println("** ERROR: runCypher ==> " + ex.getMessage());
		}
		return stream;
	}

	@Test
	public void apiGraphCypherGrammar01(){
		String script;
		long result;

		script = "match (a:person) return a, id(a) limit 2";
		result = runCypher(script).count();
		assertEquals( script+" => "+result, 2, result);

		script = "match (a {name:'marko'})-[b]->(c) return b, id(b)";
		result = runCypher(script).count();
		assertEquals( script+" => "+result, 3, result);
	}

/*
	@Test
	public void jsonSerializeTest() throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new AgensJacksonModule());

		final AgensVertex v = (AgensVertex) g.addVertex(
				T.id, "modern_1", T.label, "person"
				, "name", "marko", "age", 29, "country", "USA"
		);
		String json = mapper.writeValueAsString(v);
	}
*/

	// **참고
	// https://www.baeldung.com/spring-boot-testresttemplate
	// https://www.baeldung.com/java-assert-string-not-empty
	// Null 값 체크 ==> assertThat( mc, is(nullValue()) );
	// https://m.blog.naver.com/PostView.nhn?blogId=simpolor&logNo=221327833587&proxyReferer=https%3A%2F%2Fwww.google.com%2F
	// https://www.lesstif.com/pages/viewpage.action?pageId=18219426#hamcrest%EB%A1%9C%EA%B0%80%EB%8F%85%EC%84%B1%EC%9E%88%EB%8A%94jUnitTestCase%EB%A7%8C%EB%93%A4%EA%B8%B0-Numbers

	////////////////////////////////////////////////

	@Test
	public void managerGraphCreate() {
		AgensGraph g = AgensFactory.createEmpty(api, gName);
		final Graph newGraph = manager.openGraph(gName, (String gName) -> {
			return g;	// graph 바꿔치기
		});

		assertNotNull(g);
		assertThat(g, instanceOf(AgensGraph.class));
		// assertSame(g, newGraph);
/*
		final Vertex marko = g.addVertex(T.id, "modern_1", T.label, "person", "name", "marko", "age", 29, "country", "USA");
		final Vertex vadas = g.addVertex(T.id, "modern_2", T.label, "person", "name", "vadas", "age", 27, "country", "USA");
		final Vertex lop = g.addVertex(T.id, "modern_3", T.label, "software", "name", "lop", "lang", "java");
		final Vertex josh = g.addVertex(T.id, "modern_4", T.label, "person", "name", "josh", "age", 32, "country", "USA");
		final Vertex ripple = g.addVertex(T.id, "modern_5", T.label, "software", "name", "ripple", "lang", "java");
		final Vertex peter = g.addVertex(T.id, "modern_6", T.label, "person", "name", "peter", "age", 35, "country", "USA");
		marko.addEdge("knows", vadas, T.id, "modern_7", "weight", 0.5d);
		marko.addEdge("knows", josh, T.id, "modern_8", "weight", 1.0d);
		marko.addEdge("created", lop, T.id, "modern_9", "weight", 0.4d);
		josh.addEdge("created", ripple, T.id, "modern_10", "weight", 1.0d);
		josh.addEdge("created", lop, T.id, "modern_11", "weight", 0.4d);
		peter.addEdge("created", lop, T.id, "modern_12", "weight", 0.2d);

		try {
			Thread.sleep(500); 	// Then do something meaningful...
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		long sizeV = api.countV(gName);
		long sizeE = api.countE(gName);

		assertThat("Some vertex insert fails", sizeV, is(6L));
		assertThat("Some edge insert fails", sizeE, is(6L));

		///////////////////////////////////

		GraphTraversalSource t = g.traversal();
		assertNotNull(t);

		List<Vertex> vertexList = t.V().next(100);
		assertTrue("vertices is empty", vertexList.size() > 0);

		List<Edge> edgeList = t.E().next(100);
		assertTrue("edges is empty", edgeList.size() > 0);

		Object vid = AgensIdManager.ANY.getNextId(g);
		assertFalse("V["+vid.toString()+"] is exist", t.V(vid).hasNext());

		String[] vids = { "modern_3","modern_4","modern_5" };
		vertexList = t.V(vids).next(100);
		assertTrue("vertices by ids is not working", vertexList.size()==3);

		vid = "modern_1";
		edgeList = t.V(vid).bothE().next(100);
		assertTrue("V.bothE() is not working", edgeList.size() > 0);

		Vertex v1 = t.V(vid).next();
		vertexList = t.V(v1.id()).out().next(100);
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

		sizeV = api.countV(gName);
		sizeE = api.countE(gName);

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
		sizeV = (int) api.countV(gName);
		sizeE = (int) api.countE(gName);

		System.out.println("Removing all vertices : "+gName+".V() = "+sizeV);
		System.out.println("Removing all edges : "+gName+".E() = "+sizeE);
*/
	}

}
