package net.bitnine.agenspop.web;

import net.bitnine.agenspop.config.properties.ProductProperties;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping(value = "${agens.api.base-path}/admin")
public class ManagerController {

    private final GraphManager manager;
    private final ProductProperties productProperties;

    @Autowired
    public ManagerController(
            GraphManager manager,
            ProductProperties productProperties
    ){
        this.manager = manager;
        this.productProperties = productProperties;
    }

    private final HttpHeaders productHeaders(){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", productProperties.getName());
        headers.add("agens.product.version", productProperties.getVersion());
        return headers;
    }

    ///////////////////////////////////////////

    @GetMapping("/hello")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, admin!\"}";
    }

    @GetMapping("/graphs")
    public ResponseEntity<Map<String, String>> listGraphs() throws Exception {
        Set<String> names = manager.getGraphNames();
        Map<String, String> graphs = new HashMap<>();
        for( String name : names ){
            graphs.put(name, manager.getGraph(name).toString());
        }
        HttpStatus httpStatus = HttpStatus.OK;
        if( graphs == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<Map<String, String>>(graphs, productHeaders(), httpStatus);
    }
/*
    // reload graphs
    @GetMapping("/reload")
    public ResponseEntity<?> reloadGraphs() throws Exception {
        HttpStatus httpStatus = HttpStatus.OK;
        if( graphs == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<Map<String, String>>(graphs, productHeaders(), httpStatus);
    }

    // remove graph
    @GetMapping("/remove/{graph}")
    public ResponseEntity<?> removeGraph(@PathVariable String graph) throws Exception {
        HttpStatus httpStatus = HttpStatus.OK;
        if( graphs == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<String>(graphs, productHeaders(), httpStatus);
    }

    // list labels of graph
    @GetMapping("/labels/{graph}")
    public ResponseEntity<?> listLabels(@PathVariable String graph) throws Exception {
        HttpStatus httpStatus = HttpStatus.OK;
        if( graphs == null ) httpStatus = HttpStatus.NO_CONTENT;

        return new ResponseEntity<String>(graphs, productHeaders(), httpStatus);
    }
 */
}
