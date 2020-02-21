package net.bitnine.agenspop.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.graph.structure.AgensEdge;
// import net.bitnine.agenspop.graph.structure.AgensIoRegistryV1;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.service.AgensGremlinService;
import net.bitnine.agenspop.dto.DetachedGraph;
import net.bitnine.agenspop.util.AgensJacksonModule;
import net.bitnine.agenspop.util.AgensUtilHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RestController
@RequestMapping(value = "${agens.api.base-path}/graph")
public class GraphController {
    private static final ObjectMapper mapper = new ObjectMapper();

//    private static ObjectMapper mapperV1 = GraphSONMapper.build().
//            addRegistry(AgensIoRegistryV1.instance()).
//            addCustomModule(new AbstractGraphSONMessageSerializerV1d0.GremlinServerModule()).
//            version(GraphSONVersion.V1_0).create().createMapper();

    private final AgensGremlinService gremlin;
    private final ProductProperties productProperties;

    @Autowired
    public GraphController(
            AgensGremlinService gremlin,
            ProductProperties productProperties
    ){
        this.gremlin = gremlin;
        this.mapper.registerModule(new AgensJacksonModule());
        this.productProperties = productProperties;
    }

    ///////////////////////////////////////////

    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, graph!\"}";
    }

    @GetMapping(value="/{datasource}", produces="application/json; charset=UTF-8")
    public ResponseEntity<DetachedGraph> getGraph(@PathVariable String datasource) throws Exception {
        CompletableFuture<DetachedGraph> future = gremlin.getGraph(datasource);
        CompletableFuture.allOf(future).join();
        DetachedGraph graph = future.get();

        return new ResponseEntity<DetachedGraph>(graph
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    ///////////////////////////////////////////

    // http://localhost:8080/api/graph/gremlin?q=modern_g.V()
    @GetMapping(value="/gremlin", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> runGremlin(
            @RequestParam("q") String script
    ) throws Exception {
        if( script == null || script.length() == 0 )
            throw new IllegalAccessException("script is empty");

        // sql decoding : "+", "%", "&" etc..
        try {
            script = URLDecoder.decode(script, StandardCharsets.UTF_8.toString());
        }catch(UnsupportedEncodingException ue){
            System.out.println("api.query: UnsupportedEncodingException => "+script);
            throw new IllegalArgumentException("UnsupportedEncodingException => "+ue.getCause());
        }

        Stream<Object> stream = Stream.empty();
        try {
            CompletableFuture<?> future = gremlin.runGremlin(script);
            CompletableFuture.allOf(future).join();
            stream = (Stream<Object>) future.get();
        }catch (Exception ex){
            System.out.println("** ERROR: runGremlin ==> " + ex.getMessage());
        }
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , stream );
    }

    // http://localhost:8080/api/graph/cypher?ds=modern&q=match%20(a:person%20%7Bcountry:%20%27USA%27%7D)%20return%20a,%20id(a)%20limit%2010
    @GetMapping(value="/cypher", produces="application/stream+json; charset=UTF-8")
    public ResponseEntity<?> runCypher(
            @RequestParam("q") String script
            , @RequestParam(value="ds", required=false, defaultValue ="modern") String datasource
    ) throws Exception {
        if( script == null || script.length() == 0 )
            throw new IllegalAccessException("script is empty");

        // sql decoding : "+", "%", "&" etc..
        try {
            script = URLDecoder.decode(script, StandardCharsets.UTF_8.toString());
        }catch(UnsupportedEncodingException ue){
            System.out.println("api.query: UnsupportedEncodingException => "+script);
            throw new IllegalArgumentException("UnsupportedEncodingException => "+ue.getCause());
        }

        Stream<Object> stream = Stream.empty();
        try {
            CompletableFuture<?> future = gremlin.runCypher(script, datasource);
            CompletableFuture.allOf(future).join();
            stream = (Stream<Object>)future.get();
        }catch (Exception ex){
            System.out.println("** ERROR: runCypher ==> " + ex.getMessage());
        }
        return AgensUtilHelper.responseStream(mapper, AgensUtilHelper.productHeaders(productProperties)
                , stream );
    }

    ///////////////////////////////////////////

}
