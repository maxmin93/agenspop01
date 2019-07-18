package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public abstract class ElasticElementDocument implements ElasticElement {

    public static final String DEFAULT_DATASOURCE = "default";
    public static final String ID_DELIMITER = "_";
    // **NOTE: Except special characters ==> + - && || ! ( ) { } [ ] ^ " ~ * ? : \
    //      삽입해도 ID에 대해 전체 완전 매칭이 되는 문자로 취급되는 것은 "_" 밖에 없는듯

    @Id
    protected String id;
    @Field(type = FieldType.Boolean)
    protected Boolean deleted;
    @Version
    protected Long version;

    @Field(type = FieldType.Keyword)    // not_analyzed
    protected String label;
    @Field(type = FieldType.Keyword)    // not_analyzed
    protected String datasource;

    @Field(type = FieldType.Nested, includeInParent = true)
    protected Set<ElasticPropertyDocument> properties = ConcurrentHashMap.newKeySet();

    protected ElasticElementDocument(){
        this.deleted = false;
        this.version = System.currentTimeMillis();
    }
    protected ElasticElementDocument(String id, String label){
        this();
        this.id = id;
        this.label = label;
        this.datasource = id.split(ID_DELIMITER)[0];
    }

    @Override public int hashCode() {
        return 31*id.hashCode() + 43*label.hashCode();
    }
    @Override public boolean equals(final Object obj) {
        if (obj == null) return false;
        if (this.getClass() != obj.getClass()) return false;
        if (this == obj) return true;

        ElasticElement that = (ElasticElement) obj;
        if (this.id == null || that.getId() == null || !this.id.equals(that.getId()) )
            return false;
        if (this.label == null || that.getLabel() == null || !this.label.equals(that.getLabel()) )
            return false;
        return true;
    }

    public Long getVersion(){ return version; }

    @Override public String getId(){ return id; }
    @Override public String getLabel(){
        return label;
    }
    @Override public String getDatasource(){
        return datasource;
    }

    @Override public Iterable<String> getKeys(){
        return properties.stream().map(ElasticProperty::getKey).collect(Collectors.toList());
    }
    @Override public void setProperties(Set<ElasticPropertyDocument> properties){
        this.properties = properties;
    }
    @Override public Set<ElasticPropertyDocument> getProperties(){
        return this.properties;
    }

    /////////////////////////////////////////

    @Override
    public Optional<ElasticProperty> getProperty(String key){
        Iterator<ElasticPropertyDocument> iter = properties.stream()
                .filter(p->p.getKey().equals(key)).iterator();
        if( !iter.hasNext() ){
            return Optional.empty();
        }
        return Optional.of(iter.next());
    }

    @Override
    public ElasticProperty getProperty(String key, Object defaultValue){
        Iterator<ElasticPropertyDocument> iter = properties.stream()
                .filter(p->p.getKey().equals(key)).iterator();
        if( !iter.hasNext() ) {
            return new ElasticPropertyDocument(key, defaultValue.getClass().getName(), defaultValue);
        }
        return iter.next();
    }

    @Override
    public boolean setProperty(ElasticProperty property) {
        removeProperty(property.getKey());
        return properties.add((ElasticPropertyDocument) property);
    }

    @Override
    public ElasticProperty setProperty(String key, Object value) {
        return setProperty(key, value.getClass().getName(), value);
    }
    @Override
    public ElasticProperty setProperty(String key, String type, Object value){
        ElasticProperty property = new ElasticPropertyDocument(key, type, value);
        return setProperty(property) ? property : null;
    }

    @Override
    public void removeProperty(String key){
        Iterator<ElasticPropertyDocument> iter = properties.stream().filter(p->p.getKey().equals(key)).iterator();
        while( iter.hasNext() ){
            ElasticPropertyDocument property = iter.next();
            iter.remove();
        }
    }

    @Override public boolean hasProperty(String key){
        Iterator<ElasticPropertyDocument> iter = properties.stream().filter(p->p.getKey().equals(key)).iterator();
        return iter.hasNext();
    }

    @Override public void delete(){
        this.deleted = true;
        this.properties = null;
    }
    @Override public boolean isDeleted(){
        return deleted.booleanValue();
    }

    @Override
    public String toString() {
        return "ElasticElement{" +
                "deleted=" + deleted +
                ", id='" + id + '\'' +
                ", label='" + label + '\'' +
                ", properties=[" + properties.stream().map(ElasticProperty::getKey).collect(Collectors.joining(",")) +
                "]}";
    }
}
