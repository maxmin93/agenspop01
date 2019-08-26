package net.bitnine.agenspop.elasticgraph.model;

import lombok.Data;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.basegraph.model.BaseProperty;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class ElasticElement implements BaseElement {

    protected String id;
    protected String label;
    protected String datasource;
    protected List<ElasticProperty> properties;

    protected ElasticElement(String datasource, String id, String label){
        this.datasource = datasource;
        this.id = id;
        this.label = label;
        this.properties = new ArrayList<>();
    }

    @Override
    public List<String> keys(){
        List<String> keys = new ArrayList<>();
        for(ElasticProperty p : properties ){
            if( p.isPresent() ) keys.add(p.getKey());   // pre-check exception
        }
        return keys;
    }

    @Override
    public List<Object> values(){
        List<Object> values = new ArrayList<>();
        for(ElasticProperty p : properties ){
            if( p.isPresent() ) values.add(p.value());  // pre-check exception
        }
        return values;
    }

    @Override
    public Collection<BaseProperty> properties(){
        return new HashSet<>(properties);
    }

    @Override
    public void properties(Collection<? extends BaseProperty> properties){
        this.properties = properties.stream().map(r->(ElasticProperty)r).collect(Collectors.toList());
    }

    @Override
    public boolean hasProperty(String key){
        return keys().contains(key);
    }

    @Override
    public BaseProperty getProperty(String key){
        for( ElasticProperty property : properties ){
            if( property.getKey().equals(key) ) return property;
        }
        return null;
    }

    @Override
    public BaseProperty getProperty(String key, BaseProperty defaultProperty){
        BaseProperty property = getProperty(key);
        return property == null ? defaultProperty : property;
    }

    @Override
    public void setProperty(BaseProperty property){
        if( hasProperty(property.key()) ) removeProperty(property.key());
        properties.add((ElasticProperty) property);
    }

    @Override
    public BaseProperty removeProperty(String key){
        Iterator<ElasticProperty> iter = properties.iterator();
        while(iter.hasNext()){
            BaseProperty property = iter.next();
            if( property.key().equals(key) ){
                iter.remove();
                return property;
            }
        }
        return null;
    }
}
