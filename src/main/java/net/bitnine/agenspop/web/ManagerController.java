package net.bitnine.agenspop.web;

import net.bitnine.agenspop.service.AgensGremlinService;
import net.bitnine.agenspop.service.DetachedGraph;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping(value = "${agens.api.base-path}/admin")
public class ManagerController {

    private final GraphManager manager;

    @Autowired
    public ManagerController(GraphManager manager){
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
        return "{ \"msg\": \"Hello, admin!\"}";
    }

    @GetMapping("/graphs")
    public ResponseEntity<Map<String, String>> graphTest0() throws Exception {
        Set<String> names = manager.getGraphNames();
        Map<String, String> graphs = new HashMap<>();
        for( String name : names ){
            graphs.put(name, manager.getGraph(name).toString());
        }
        HttpStatus httpStatus = HttpStatus.OK;
        if( graphs == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<Map<String, String>>(graphs, productHeaders(), httpStatus);
    }
}
