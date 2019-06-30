package net.bitnine.agenspop.web;

import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensIoRegistryV1;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.service.AgensGremlinService;
import net.bitnine.agenspop.service.DetachedGraph;
import org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "${agens.api.base-path}/graph")
public class GraphController {

    private static final ObjectMapper mapper = GraphSONMapper.build()
            .version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES)
            .create().createMapper();

    private static ObjectMapper mapperV1 = GraphSONMapper.build().
            addRegistry(AgensIoRegistryV1.instance()).
            addCustomModule(new AbstractGraphSONMessageSerializerV1d0.GremlinServerModule()).
            version(GraphSONVersion.V1_0).create().createMapper();

    private final AgensGremlinService gremlin;
    private final GraphManager manager;
    private final String gName = "modern";

    @Autowired
    public GraphController(GraphManager manager, AgensGremlinService gremlin){
        this.gremlin = gremlin;
        this.manager = manager;
    }

    private final HttpHeaders productHeaders(){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", "agenspop");
        headers.add("agens.product.version", "0.1");
        return headers;
    }

    ///////////////////////////////////////////

    @GetMapping("/hello")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, graph!\"}";
    }

    @GetMapping("/{datasource}/v")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<?> listAllV(@PathVariable String datasource) throws Exception {
        AgensGraph g = (AgensGraph) this.manager.getGraph(datasource);
        if( g == null ) throw new IllegalAccessException(String.format("graph[%s] is not found.", datasource));

        List<AgensVertex> vertices = new ArrayList<>();
        Iterator<Vertex> iter = g.vertices();
        while( iter.hasNext() ){
            vertices.add((AgensVertex) iter.next());
        }

        String json = "{}";
        json = mapperV1.writeValueAsString(vertices);     // AgensIoRegistryV1
        return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
    }

    @GetMapping("/{datasource}/e")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<?> listAllE(@PathVariable String datasource) throws Exception {
        AgensGraph g = (AgensGraph) this.manager.getGraph(datasource);
        if( g == null ) throw new IllegalAccessException(String.format("graph[%s] is not found.", datasource));

        List<AgensEdge> edges = new ArrayList<>();
        Iterator<Edge> iter = g.edges();
        while( iter.hasNext() ){
            edges.add((AgensEdge)iter.next());
        }

        String json = "{}";
        json = mapperV1.writeValueAsString(edges);     // AgensIoRegistryV1
        return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
    }

    ///////////////////////////////////////////

    @GetMapping("/gtest0")
    public ResponseEntity<DetachedGraph> graphTest0() throws Exception {
        CompletableFuture<DetachedGraph> future = gremlin.getGraph("modern");
        CompletableFuture.allOf(future).join();
        DetachedGraph graph = future.get();

        HttpStatus httpStatus = HttpStatus.OK;
        if( graph == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<DetachedGraph>(graph, productHeaders(), httpStatus);
    }

    @GetMapping("/gtest1")
    public ResponseEntity<List<Vertex>> graphTest1() throws Exception {
        CompletableFuture<List<Vertex>> future = gremlin.getVertices("modern");
        CompletableFuture.allOf(future).join();
        List<Vertex> vertices = future.get();

        HttpStatus httpStatus = HttpStatus.OK;
        if( vertices == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<List<Vertex>>(vertices, productHeaders(), httpStatus);
    }

    @GetMapping("/gtest2")
    public ResponseEntity<List<Edge>> graphTest2() throws Exception {
        CompletableFuture<List<Edge>> future = gremlin.getEdges("modern");
        CompletableFuture.allOf(future).join();
        List<Edge> edges = future.get();

        HttpStatus httpStatus = HttpStatus.OK;
        if( edges == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<List<Edge>>(edges, productHeaders(), httpStatus);
    }

    /////////////////////////////////////////

    @GetMapping("/test0")
    @ResponseStatus(HttpStatus.OK)
    public String test0() throws Exception {
        AgensGraph g = (AgensGraph) this.manager.getGraph(gName);
        if( g == null ) throw new IllegalAccessException(String.format("graph[%s] is not found.", gName));

        String json = "{}";
        json = mapperV1.writeValueAsString(g);     // AgensIoRegistryV1
        // for DEBUG
        System.out.println(String.format("++ G(%s) ==> {%s}", g.toString(), json));

        return json;
    }

    @GetMapping("/test1")
    @ResponseStatus(HttpStatus.OK)
    public String test1() throws Exception {
        String tsName = gName + "_traversal";
        GraphTraversalSource ts = (GraphTraversalSource) this.manager.getTraversalSource(tsName);
        if( ts == null ) throw new IllegalAccessException(tsName + " is not found.");

        List<Vertex> vList = ts.V().hasLabel("person").next(5);
        List<AgensVertex> avList = vList.stream().map(v->(AgensVertex)v).collect(Collectors.toList());
        String json = "[]";
        if( vList != null ){
            json = mapperV1.writeValueAsString(avList);     // AgensIoRegistryV1
            System.out.println(String.format("++ V(%d) ==> {%s}", avList.size(), json));
        }
        return json;
    }

    @GetMapping("/test2")
    @ResponseStatus(HttpStatus.OK)
    public String test2() throws Exception {
        String tsName = gName + "_traversal";
        GraphTraversalSource ts = (GraphTraversalSource) this.manager.getTraversalSource(tsName);
        if( ts == null ) throw new IllegalAccessException(tsName + " is not found.");

        List<Vertex> vList = ts.V().has("person", "name","marko").next(5);
        List<AgensVertex> avList = vList.stream().map(v->(AgensVertex)v).collect(Collectors.toList());
        String json = "[]";
        if( vList != null ){
            json = mapperV1.writeValueAsString(avList);     // AgensIoRegistryV1
            System.out.println(String.format("++ V(%d) ==> {%s}", avList.size(), json));
        }
        return json;
    }

}
