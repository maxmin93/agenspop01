package net.bitnine.agens.agenspop.web;

import net.bitnine.agens.agenspop.graph.AgensGraphManager;
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

@RestController
@RequestMapping(value = "${agens.api.base-path}/graph")
public class GraphController {

    private static final ObjectMapper mapper = GraphSONMapper.build()
            .version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES)
            .create().createMapper();

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

    @GetMapping("/test1")
    @ResponseStatus(HttpStatus.OK)
    public String test1() throws Exception {
        String tsName = gName + "_traversal";
        GraphTraversalSource ts = (GraphTraversalSource) this.manager.getTraversalSource(tsName);
        if( ts == null ) throw new IllegalAccessException(tsName + " is not found.");

        List<Vertex> vList = ts.V().limit(5).next(5);
        String json = "[]";
        if( vList != null ){
            json = mapper.writeValueAsString(vList);
            System.out.println(String.format("++ V(%d) ==> {%s}", vList.size(), json));
        }
        return json;
    }

}
