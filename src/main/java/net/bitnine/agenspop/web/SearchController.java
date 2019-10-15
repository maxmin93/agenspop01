package net.bitnine.agenspop.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.elasticgraph.ElasticGraphAPI;
import net.bitnine.agenspop.elasticgraph.model.ElasticEdge;
import net.bitnine.agenspop.elasticgraph.model.ElasticVertex;
import net.bitnine.agenspop.util.AgensJacksonModule;
import net.bitnine.agenspop.util.AgensUtilHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.bitnine.agenspop.util.AgensUtilHelper.wrapException;

@Slf4j
@RestController
@RequestMapping(value = "${agens.api.base-path}/search")
public class SearchController {
    private final ElasticGraphAPI base;
    private final ObjectMapper mapper;
    private final ProductProperties productProperties;

    @Autowired
    public SearchController(
            ElasticGraphAPI base, ObjectMapper objectMapper, ProductProperties productProperties
    ){
        this.base = base;
        this.mapper = objectMapper;
        this.mapper.registerModule(new AgensJacksonModule());
        this.productProperties = productProperties;
    }

    @GetMapping(value="/test", produces="application/json; charset=UTF-8")
    public String test(){
        return "{ \"test\": \"Success\" }";
    }

    ///////////////////////////////////////////////////////////////

    @GetMapping(value="/count", produces="application/json; charset=UTF-8")
    public ResponseEntity count() throws Exception {
        return new ResponseEntity(base.count(), HttpStatus.OK);
    }
    @GetMapping(value="/{datasource}/count", produces="application/json; charset=UTF-8")
    public ResponseEntity count(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.count(datasource), HttpStatus.OK);
    }
    @GetMapping(value="/{datasource}/labels", produces="application/json; charset=UTF-8")
    public ResponseEntity labels(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.labels(datasource), HttpStatus.OK);
    }

    @GetMapping(value="/{datasource}/v/{label}/keys", produces="application/json; charset=UTF-8")
    public ResponseEntity vertexLabelKeys(@PathVariable String datasource, @PathVariable String label) throws Exception {
        return new ResponseEntity(base.listVertexLabelKeys(datasource, label), HttpStatus.OK);
    }
    @GetMapping(value="/{datasource}/e/{label}/keys", produces="application/json; charset=UTF-8")
    public ResponseEntity edgeLabelKeys(@PathVariable String datasource, @PathVariable String label) throws Exception {
        return new ResponseEntity(base.listEdgeLabelKeys(datasource, label), HttpStatus.OK);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/v/v01"
curl -X GET "localhost:8080/elastic/e/e01"
    */
    @GetMapping(value="/v/{id}", produces="application/json; charset=UTF-8")
    public ResponseEntity findV(@PathVariable String id) throws Exception {
        Optional<BaseVertex> v = base.getVertexById(id);
        String json = !v.isPresent() ? "{}" : mapper.writeValueAsString(v);
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value="/e/{id}", produces="application/json; charset=UTF-8")
    public ResponseEntity findE(@PathVariable String id) throws Exception {
        Optional<BaseEdge> e = base.getEdgeById(id);
        String json = !e.isPresent() ? "{}" : mapper.writeValueAsString(e);
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    /*
curl -X GET "localhost:8080/elastic/sample/v"
curl -X GET "localhost:8080/elastic/sample/e"
    */
/*
    @GetMapping(value="/{datasource}/v", produces="application/json; charset=UTF-8")
    public ResponseEntity findV_All(@PathVariable String datasource) throws Exception {
        String json = mapper.writeValueAsString( base.vertices(datasource) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }
    @GetMapping(value="/{datasource}/e", produces="application/json; charset=UTF-8")
    public ResponseEntity findE_All(@PathVariable String datasource) throws Exception {
        String json = mapper.writeValueAsString( base.edges(datasource) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }
*/
    // stream of JSON lines
    @GetMapping(value="/{datasource}/v", produces="application/stream+json; charset=UTF-8")
    public Flux<String> findV_All(@PathVariable String datasource) throws Exception {
        Stream<String> vstream = base.vertexStream(datasource).map(r ->
                wrapException(()-> mapper.writeValueAsString(r)+",")
            );
        return Flux.fromStream( Stream.concat(Stream.concat(Stream.of("["), vstream), Stream.of("]")) );
    }
    @GetMapping(value="/{datasource}/e", produces="application/stream+json; charset=UTF-8")
    public Flux<String> findE_All(@PathVariable String datasource) throws Exception {
        Stream<String> estream = base.edgeStream(datasource).map(r ->
                wrapException(()-> mapper.writeValueAsString(r)+",")
            );
        return Flux.fromStream( Stream.concat(Stream.concat(Stream.of("["), estream), Stream.of("]")) );
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/sample/v/label?q=person"
curl -X GET "localhost:8080/elastic/sample/e/label?q=person"
    */
    @GetMapping(value = "/{datasource}/v/labels")
    public ResponseEntity findV_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        String json = labels.size() == 0 ? "[]" : mapper.writeValueAsString(
                base.findVertices(datasource, labels.toArray(array)) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value = "/{datasource}/e/labels")
    public ResponseEntity findE_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        String json = labels.size() == 0 ? "[]" : mapper.writeValueAsString(
                base.findEdges(datasource, labels.toArray(array)) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }


    @GetMapping(value = "/{datasource}/v/keys")
    public ResponseEntity findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        String json = keys.size() == 0 ? "[]" : mapper.writeValueAsString(
                base.findVerticesWithKeys(datasource, keys.toArray(array)) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value = "/{datasource}/e/keys")
    public ResponseEntity findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        String json = keys.size() == 0 ? "[]" : mapper.writeValueAsString(
                base.findEdgesWithKeys(datasource, keys.toArray(array)) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }


    @GetMapping(value = "/{datasource}/v/key")
    public ResponseEntity findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key,
            @RequestParam(value = "hasNot", required=false, defaultValue="false") boolean hasNot
    ) throws Exception {
        String json = mapper.writeValueAsString( base.findVertices(datasource, key, hasNot) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value = "/{datasource}/e/key")
    public ResponseEntity findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key,
            @RequestParam(value = "hasNot", required=false, defaultValue="false") boolean hasNot
    ) throws Exception {
        String json = mapper.writeValueAsString( base.findEdges(datasource, key, hasNot) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }


    @GetMapping(value = "/{datasource}/v/values")
    public ResponseEntity findV_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        String json = values.size() == 0 ? "[]" : mapper.writeValueAsString(
                base.findVerticesWithValues(datasource, values.toArray(array)) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value = "/{datasource}/e/values")
    public ResponseEntity findE_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        String json = values.size() == 0 ? "[]" : mapper.writeValueAsString(
                base.findEdgesWithValues(datasource, values.toArray(array)) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }


    // http://localhost:8080/api/search/modern/v/value?q=ja
    @GetMapping(value = "/{datasource}/v/value")
    public ResponseEntity findV_PropertyValuePartial(
            @PathVariable String datasource,
            @RequestParam(value = "q") String value
    ) throws Exception {
        String json = mapper.writeValueAsString( base.findV_DatasourceAndPropertyValuePartial(datasource, value) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }
    // http://localhost:8080/api/search/modern/e/value?q=0.
    @GetMapping(value = "/{datasource}/e/value")
    public ResponseEntity findE_PropertyValuePartial(
            @PathVariable String datasource,
            @RequestParam(value = "q") String value
    ) throws Exception {
        String json = mapper.writeValueAsString( base.findE_DatasourceAndPropertyValuePartial(datasource, value) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    ////////////////////////////////////////////////

    // http://localhost:8080/api/search/modern/v/keyvalue?key=name&value=java
    @GetMapping(value = "/{datasource}/v/keyvalue")
    public ResponseEntity findV_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        String json = mapper.writeValueAsString( base.findVertices(datasource, key, value) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }
    // http://localhost:8080/api/search/modern/e/keyvalue?key=weight&value=0.5
    @GetMapping(value = "/{datasource}/e/keyvalue")
    public ResponseEntity findE_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        String json = mapper.writeValueAsString( base.findEdges(datasource, key, value) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }


    @GetMapping(value = "/{datasource}/v/labelkeyvalue")
    public ResponseEntity findV_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        String json = mapper.writeValueAsString( base.findVertices(datasource, label, key, value) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value = "/{datasource}/e/labelkeyvalue")
    public ResponseEntity findE_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        String json = mapper.writeValueAsString( base.findEdges(datasource, label, key, value) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    // http://localhost:8080/api/search/modern/v/hasContainers?label=person&keyNot=gender&key=country&values=USA
    // http://localhost:8080/api/search/modern/v/hasContainers?label=person&kvPairs=country@USA,name@marko
    @GetMapping(value = "/{datasource}/v/hasContainers")
    public ResponseEntity findV_hasContainers(
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

        String json = mapper.writeValueAsString( ((ElasticGraphAPI)base).findVertices(
                datasource, label, labels, key, keyNot, keys, values, kvPairs) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

    @GetMapping(value = "/{datasource}/e/hasContainers")
    public ResponseEntity findE_hasContainers(
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

        String json = mapper.writeValueAsString( ((ElasticGraphAPI)base).findEdges(
                datasource, label, labels, key, keyNot, keys, values, kvPairs) );
        return new ResponseEntity(json, AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }

}
