package net.bitnine.agenspop.web;

import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.graph.AgensGraphManager;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping(value = "${agens.api.base-path}/admin")
public class ManagerController {

    private final AgensGraphManager manager;
    private final ProductProperties productProperties;

    @Autowired
    public ManagerController(
            AgensGraphManager manager,
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

    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        return "{ \"msg\": \"Hello, admin!\"}";
    }

    @GetMapping(value="/graphs", produces="application/json; charset=UTF-8")
    public ResponseEntity<Map<String, String>> listGraphs() throws Exception {
        Set<String> names = manager.getGraphNames();
        Map<String, String> graphs = new HashMap<>();
        for( String name : names ){
            graphs.put(name, manager.getGraph(name).toString());
        }
        return new ResponseEntity<Map<String, String>>(graphs, productHeaders(), HttpStatus.OK);
    }

    @GetMapping(value="/labels", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> listLabels() throws Exception {
        Map<String,Map<String,Long>> labels = manager.getGraphLabels();
        return new ResponseEntity<Map<String,Map<String,Long>>>(labels, productHeaders(), HttpStatus.OK);
    }

    @GetMapping(value="/keys/{datasource}", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> listKeys(@PathVariable String datasource) throws Exception {
        Map<String,Map<String,Long>> keys = manager.getGraphKeys(datasource);
        return new ResponseEntity<Map<String,Map<String,Long>>>(keys, productHeaders(), HttpStatus.OK);
    }

    // remove graph
    @GetMapping(value="/remove/{graph}", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> removeGraph(@PathVariable String graph) throws Exception {
        Graph g = manager.removeGraph(graph);

        String message = "{\"remove\" : \""+(g==null ? "null":g.toString())+"\"}";
        return new ResponseEntity<String>(message, productHeaders(), HttpStatus.OK);
    }

    // reload graphs
    @GetMapping(value="/update", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> updateGraphs() throws Exception {
        manager.updateGraphs();
        return new ResponseEntity<Map<String,String>>( manager.getGraphStates(), productHeaders(), HttpStatus.OK);
    }

    /*

    // list labels of graph
    @GetMapping(value="/labels/{graph}", produces="application/json; charset=UTF-8")
    public ResponseEntity<?> listLabels(@PathVariable String graph) throws Exception {

        return new ResponseEntity<String>(graphs, productHeaders(), HttpStatus.OK);
    }
 */
}
