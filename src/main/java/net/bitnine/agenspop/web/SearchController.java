package net.bitnine.agenspop.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.elasticgraph.ElasticGraphAPI;
import net.bitnine.agenspop.elasticgraph.model.ElasticEdge;
import net.bitnine.agenspop.elasticgraph.model.ElasticVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping(value = "${agens.api.base-path}/search")
public class SearchController {
    private final ElasticGraphAPI base;
    private final ObjectMapper mapper;

    @Autowired
    public SearchController(ElasticGraphAPI base, ObjectMapper mapper){
        this.base = base;
        this.mapper = mapper;
    }

    @GetMapping("/test")
    public String test(){
        return "Success";
    }

    ///////////////////////////////////////////////////////////////

    @GetMapping("/count")
    public ResponseEntity count() throws Exception {
        return new ResponseEntity(base.count(), HttpStatus.OK);
    }
    @GetMapping("/{datasource}/count")
    public ResponseEntity count(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.count(datasource), HttpStatus.OK);
    }
    @GetMapping("/{datasource}/labels")
    public ResponseEntity labels(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.labels(datasource), HttpStatus.OK);
    }

    @GetMapping("/{datasource}/v/{label}/keys")
    public ResponseEntity vertexLabelKeys(@PathVariable String datasource, @PathVariable String label) throws Exception {
        return new ResponseEntity(base.listVertexLabelKeys(datasource, label), HttpStatus.OK);
    }
    @GetMapping("/{datasource}/e/{label}/keys")
    public ResponseEntity edgeLabelKeys(@PathVariable String datasource, @PathVariable String label) throws Exception {
        return new ResponseEntity(base.listEdgeLabelKeys(datasource, label), HttpStatus.OK);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/v/v01"
curl -X GET "localhost:8080/elastic/e/e01"
    */
    @GetMapping("/v/{id}")
    public Optional<BaseVertex> findV(@PathVariable String id) throws Exception {
        Optional<BaseVertex> d = base.getVertexById(id);
        if( d.isPresent() ) {
            for (BaseProperty p : d.get().properties()) {
                System.out.println(p.key() + " = " + p.value().toString());
            }
        }
        return d;
    }
    @GetMapping("/e/{id}")
    public Optional<BaseEdge> findE(@PathVariable String id) throws Exception {
        return base.getEdgeById(id);
    }

    /*
curl -X GET "localhost:8080/elastic/sample/v"
curl -X GET "localhost:8080/elastic/sample/e"
    */
    @GetMapping("/{datasource}/v")
    public Collection<BaseVertex> findV_All(@PathVariable String datasource) throws Exception {
        return base.vertices(datasource);
    }
    @GetMapping("/{datasource}/e")
    public Collection<BaseEdge> findE_All(@PathVariable String datasource) throws Exception {
        return base.edges(datasource);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/sample/v/label?q=person"
curl -X GET "localhost:8080/elastic/sample/e/label?q=person"
    */
    @GetMapping(value = "/{datasource}/v/labels")
    public Collection<BaseVertex> findV_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        return base.findVertices(datasource, labels.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/labels")
    public Collection<BaseEdge> findE_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        return base.findEdges(datasource, labels.toArray(array));
    }


    @GetMapping(value = "/{datasource}/v/keys")
    public Collection<BaseVertex> findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        return base.findVerticesWithKeys(datasource, keys.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/keys")
    public Collection<BaseEdge> findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        return base.findEdgesWithKeys(datasource, keys.toArray(array));
    }


    @GetMapping(value = "/{datasource}/v/key")
    public Collection<BaseVertex> findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key,
            @RequestParam(value = "hasNot", required=false, defaultValue="true") boolean hasNot
    ) throws Exception {
        return base.findVertices(datasource, key, hasNot);
    }
    @GetMapping(value = "/{datasource}/e/key")
    public Collection<BaseEdge> findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key,
            @RequestParam(value = "hasNot", required=false, defaultValue="true") boolean hasNot
    ) throws Exception {
        return base.findEdges(datasource, key, hasNot);
    }


    @GetMapping(value = "/{datasource}/v/values")
    public Collection<BaseVertex> findV_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        return base.findVerticesWithValues(datasource, values.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/values")
    public Collection<BaseEdge> findE_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        return base.findEdgesWithValues(datasource, values.toArray(array));
    }


    @GetMapping(value = "/{datasource}/v/value")
    public List<ElasticVertex> findV_PropertyValuePartial(
            @PathVariable String datasource,
            @RequestParam(value = "q") String value
    ) throws Exception {
        return base.findV_DatasourceAndPropertyValuePartial(datasource, value);
    }
    @GetMapping(value = "/{datasource}/e/value")
    public List<ElasticEdge> findE_PropertyValuePartial(
            @PathVariable String datasource,
            @RequestParam(value = "q") String value
    ) throws Exception {
        return base.findE_DatasourceAndPropertyValuePartial(datasource, value);
    }

    ////////////////////////////////////////////////


    @GetMapping(value = "/{datasource}/v/keyvalue")
    public Collection<BaseVertex> findV_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findVertices(datasource, key, value);
    }
    @GetMapping(value = "/{datasource}/e/keyvalue")
    public Collection<BaseEdge> findE_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findEdges(datasource, key, value);
    }


    @GetMapping(value = "/{datasource}/v/labelkeyvalue")
    public Collection<BaseVertex> findV_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findVertices(datasource, label, key, value);
    }
    @GetMapping(value = "/{datasource}/e/labelkeyvalue")
    public Collection<BaseEdge> findE_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findEdges(datasource, label, key, value);
    }


    @GetMapping(value = "/{datasource}/v/hasContainers")
    public Collection<BaseVertex> findV_hasContainers(
            @PathVariable String datasource,
            @RequestParam(value = "label", required = false) String label,
            @RequestParam(value = "labels", required = false) List<String> labelParams,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "keyNot", required = false) String keyNot,
            @RequestParam(value = "keys", required = false) List<String> keyParams,
            @RequestParam(value = "values", required = false) List<String> valueParams,
            @RequestParam(value = "kvPairs", required = false) List<String> kvParams
    ) throws Exception {
        // Parameters
        String[] labels = labelParams==null ? null : labelParams.stream().toArray(String[]::new);
        String[] keys = keyParams==null ? null : keyParams.stream().toArray(String[]::new);
        String[] values = valueParams==null ? null : valueParams.stream().toArray(String[]::new);

        Map<String,String> kvPairs = null;
        if( kvParams != null && kvParams.size() > 0 ){
            final String delimter = "@";
            kvPairs = kvParams.stream()
                    .map(r->r.split(delimter,2)).filter(r->r.length==2)
                    .collect(Collectors.toMap(r->r[0],r->r[1]));
        }

        // for DEBUG
        System.out.println("V.hasContainers :: datasource => "+datasource);
        System.out.println("  , label => "+label);
        System.out.println("  , labels => "+(labels==null ? "null" : String.join(",", labels)));
        System.out.println("  , key => "+key);
        System.out.println("  , keyNot => "+keyNot);
        System.out.println("  , keys => "+(keys==null ? "null" : String.join(",", keys)));
        System.out.println("  , values => "+(values==null ? "null" : String.join(",", values)));
        System.out.println("  , kvPairs => "+(kvPairs==null ? "null" : kvPairs.entrySet().stream().map(r->r.getKey()+"="+r.getValue()).collect(Collectors.joining(","))));

        return ((ElasticGraphAPI)base).findVertices(datasource
                    , label, labels, key, keyNot, keys, values, kvPairs);
    }

    @GetMapping(value = "/{datasource}/e/hasContainers")
    public Collection<BaseEdge> findE_hasContainers(
            @PathVariable String datasource,
            @RequestParam(value = "label", required = false) String label,
            @RequestParam(value = "labels", required = false) List<String> labelParams,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "keyNot", required = false) String keyNot,
            @RequestParam(value = "keys", required = false) List<String> keyParams,
            @RequestParam(value = "values", required = false) List<String> valueParams,
            @RequestParam(value = "kvPairs", required = false) List<String> kvParams
    ) throws Exception {
        // Parameters
        String[] labels = labelParams==null ? null : labelParams.stream().toArray(String[]::new);
        String[] keys = keyParams==null ? null : keyParams.stream().toArray(String[]::new);
        String[] values = valueParams==null ? null : valueParams.stream().toArray(String[]::new);

        Map<String,String> kvPairs = null;
        if( kvParams != null && kvParams.size() > 0 ){
            final String delimter = "@";
            kvPairs = kvParams.stream()
                    .map(r->r.split(delimter)).filter(r->r.length==2)
                    .collect(Collectors.toMap(r->r[0],r->r[1]));
        }

        // for DEBUG
        System.out.println("E.hasContainers :: datasource => "+datasource);
        System.out.println("  , label => "+label);
        System.out.println("  , labels => "+(labels==null ? "null" : String.join(",", labels)));
        System.out.println("  , key => "+key);
        System.out.println("  , keyNot => "+keyNot);
        System.out.println("  , keys => "+(keys==null ? "null" : String.join(",", keys)));
        System.out.println("  , values => "+(values==null ? "null" : String.join(",", values)));
        System.out.println("  , kvPairs => "+(kvPairs==null ? "null" : kvPairs.entrySet().stream().map(r->r.getKey()+"="+r.getValue()).collect(Collectors.joining(","))));

        return ((ElasticGraphAPI)base).findEdges(datasource
                , label, labels, key, keyNot, keys, values, kvPairs);
    }

}
