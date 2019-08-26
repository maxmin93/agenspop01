package net.bitnine.agenspop.web;

import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensIoRegistryV1;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.service.AgensGremlinService;
import net.bitnine.agenspop.dto.DetachedGraph;
import org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
    private final ProductProperties productProperties;

    @Autowired
    public GraphController(
            AgensGremlinService gremlin,
            ProductProperties productProperties
    ){
        this.gremlin = gremlin;
        this.productProperties = productProperties;
    }

    private final HttpHeaders productHeaders(){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", productProperties.getName());
        headers.add("agens.product.version", productProperties.getVersion());
        return headers;
    }

    ///////////////////////////////////////////

    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, graph!\"}";
    }

    @GetMapping(value="/{datasource}", produces="application/json; charset=UTF-8")
    public ResponseEntity<DetachedGraph> graphData(@PathVariable String datasource) throws Exception {
        CompletableFuture<DetachedGraph> future = gremlin.getGraph(datasource);
        CompletableFuture.allOf(future).join();
        DetachedGraph graph = future.get();

        return new ResponseEntity<DetachedGraph>(graph, productHeaders(), HttpStatus.OK);
    }

    @GetMapping(value="/{datasource}/v", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> listVertices(@PathVariable String datasource
            , @RequestParam(value="id", required=false, defaultValue = "") List<String> ids
            , @RequestParam(value="label", required=false, defaultValue = "") List<String> labels
            , @RequestParam(value="key", required=false, defaultValue = "") List<String> keys
            , @RequestParam(value="value", required=false, defaultValue = "") List<Object> values
    ) throws Exception {
        CompletableFuture<List<AgensVertex>> future =
                gremlin.getVertices(datasource, ids, labels, keys, values);
        CompletableFuture.allOf(future).join();
        List<AgensVertex> vertices = future.get();

        String json = "{}";
        json = mapperV1.writeValueAsString(vertices);     // AgensIoRegistryV1
        return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
    }

    @GetMapping(value="/{datasource}/e", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> listEdges(@PathVariable String datasource
            , @RequestParam(value="id", required=false, defaultValue = "") List<String> ids
            , @RequestParam(value="label", required=false, defaultValue = "") List<String> labels
            , @RequestParam(value="key", required=false, defaultValue = "") List<String> keys
            , @RequestParam(value="value", required=false, defaultValue = "") List<Object> values
    ) throws Exception {
        CompletableFuture<List<AgensEdge>> future =
                gremlin.getEdges(datasource, ids, labels, keys, values);
        CompletableFuture.allOf(future).join();
        List<AgensEdge> edges = future.get();

        String json = "{}";
        json = mapperV1.writeValueAsString(edges);     // AgensIoRegistryV1
        return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
    }

    ///////////////////////////////////////////

    @GetMapping(value="/gremlin", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> runGremlin(@RequestParam("q") String script) throws Exception {
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

            json = mapperV1.writeValueAsString(result);     // AgensIoRegistryV1
            return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
        }catch (Exception ex){
            System.out.println("** ERROR: runGremlin ==> " + ex.getMessage());
        }
        return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
    }

    @GetMapping(value="/cypher", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> runCypher(@RequestParam("q") String script
            , @RequestParam(value="ds", required=false, defaultValue ="modern") String datasource) throws Exception {
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
            CompletableFuture<?> future = gremlin.runCypher(script, datasource);
            CompletableFuture.allOf(future).join();
            Object result = future.get();

            json = mapperV1.writeValueAsString(result);     // AgensIoRegistryV1
            return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
        }catch (Exception ex){
            System.out.println("** ERROR: runCypher ==> " + ex.getMessage());
        }
        return new ResponseEntity<String>(json, productHeaders(), HttpStatus.OK);
    }

    ///////////////////////////////////////////

}
