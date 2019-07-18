package net.bitnine.agenspop.web;

import net.bitnine.agenspop.graph.AgensGraphManager;
import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensIoRegistryV1;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.service.AgensGremlinService;
import net.bitnine.agenspop.web.dto.DetachedGraph;
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
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
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
    private final AgensGraphManager manager;
    private final String gName = "modern";

    @Autowired
    public GraphController(AgensGraphManager manager, AgensGremlinService gremlin){
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

    @GetMapping("/v/{datasource}")
    public ResponseEntity<?> listVertices(@PathVariable String datasource
            , @RequestParam(value="labels", required=false, defaultValue = "") List<String> labels
            , @PageableDefault(sort={"id"}, value = 50) Pageable pageable) throws Exception {
        AgensGraph g = (AgensGraph) this.manager.getGraph(datasource);
        if( g == null ) throw new IllegalAccessException(String.format("graph[%s] is not found.", datasource));

        CompletableFuture<List<AgensVertex>> future =
                gremlin.getVertices(datasource, labels.toArray(new String[labels.size()]));
        CompletableFuture.allOf(future).join();
        List<AgensVertex> vertices = future.get();

        String json = "{}";
        json = mapperV1.writeValueAsString(vertices);     // AgensIoRegistryV1
        return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
    }

    @GetMapping("/e/{datasource}")
    public ResponseEntity<?> listEdges(@PathVariable String datasource
            , @RequestParam(value="labels", required=false, defaultValue = "") List<String> labels
            , @PageableDefault(sort={"id"}, value = 50) Pageable pageable) throws Exception {
        AgensGraph g = (AgensGraph) this.manager.getGraph(datasource);
        if( g == null ) throw new IllegalAccessException(String.format("graph[%s] is not found.", datasource));

        CompletableFuture<List<AgensEdge>> future =
                gremlin.getEdges(datasource, labels.toArray(new String[labels.size()]));
        CompletableFuture.allOf(future).join();
        List<AgensEdge> edges = future.get();

        String json = "{}";
        json = mapperV1.writeValueAsString(edges);     // AgensIoRegistryV1
        return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
    }

    @GetMapping("/script")
    public ResponseEntity<?> runScript(@RequestParam("q") String script
            , @PageableDefault(sort={"id"}, value = 50) Pageable pageable) throws Exception {
        if( script == null || script.length() == 0 )
            throw new IllegalAccessException("script is empty");

        // sql decoding : "+", "%", "&" etc..
        try {
            script = URLDecoder.decode(script, StandardCharsets.UTF_8.toString());
        }catch(UnsupportedEncodingException ue){
            System.out.println("api.query: UnsupportedEncodingException => "+script);
            throw new IllegalArgumentException("UnsupportedEncodingException => "+ue.getCause());
        }

        String json = "{}";
        try {
            CompletableFuture<?> future = gremlin.runGremlin(script);
            CompletableFuture.allOf(future).join();

            Object result = future.get();
            if( result != null ){
                if( result instanceof List )
                    ((List<Object>)result).stream().forEach(r -> System.out.println("  ==> "+r.toString()));
            }

            json = mapperV1.writeValueAsString(result);     // AgensIoRegistryV1
            return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
        }catch (Exception ex){
            System.out.println("** ERROR: runScript ==> " + ex.getMessage());
        }
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
    public ResponseEntity<List<AgensVertex>> graphTest1() throws Exception {
        CompletableFuture<List<AgensVertex>> future = gremlin.getVertices("modern");
        CompletableFuture.allOf(future).join();
        List<AgensVertex> vertices = future.get();

        HttpStatus httpStatus = HttpStatus.OK;
        if( vertices == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<List<AgensVertex>>(vertices, productHeaders(), httpStatus);
    }

    @GetMapping("/gtest2")
    public ResponseEntity<List<AgensEdge>> graphTest2() throws Exception {
        CompletableFuture<List<AgensEdge>> future = gremlin.getEdges("modern");
        CompletableFuture.allOf(future).join();
        List<AgensEdge> edges = future.get();

        HttpStatus httpStatus = HttpStatus.OK;
        if( edges == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<List<AgensEdge>>(edges, productHeaders(), httpStatus);
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
        String tsName = AgensGraphManager.GRAPH_TRAVERSAL_NAME.apply(gName);
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
        String tsName = AgensGraphManager.GRAPH_TRAVERSAL_NAME.apply(gName);
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
