package net.bitnine.agens.agenspop.web;

import net.bitnine.agens.agenspop.graph.AgensGraphManager;
import net.bitnine.agens.agenspop.graph.structure.AgensGraph;
import net.bitnine.agens.agenspop.graph.structure.AgensIoRegistryV1;
import net.bitnine.agens.agenspop.graph.structure.AgensVertex;
import org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
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

    private final GraphManager manager;
    private final String gName = "modern";

    @Autowired
    public GraphController(GraphManager manager){
        this.manager = manager;
    }

    @GetMapping("/hello")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, graph!\"}";
    }

    @GetMapping("/test0")
    @ResponseStatus(HttpStatus.OK)
    public String test0() throws Exception {
        AgensGraph g = (AgensGraph) this.manager.getGraph(gName);
        if( g == null ) throw new IllegalAccessException(String.format("graph[%s] is not found.", gName));

        String json = "[]";
        json = mapperV1.writeValueAsString(g);     // AgensIoRegistryV1
        // for DEBUG
        // System.out.println(String.format("++ G(%s) ==> {%s}", g.toString(), json));

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
//            System.out.println(String.format("++ V(%d) ==> {%s}", avList.size(), json));
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
//            System.out.println(String.format("++ V(%d) ==> {%s}", avList.size(), json));
        }
        return json;
    }

}
