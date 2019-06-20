package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticEmptyProperty;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import javax.swing.text.html.Option;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


public abstract class ElasticElementDocument implements ElasticElement {

    public static final String DEFAULT_DATASOURCE = "default";
    public static final String ID_DELIMITER = "::";

    @Id
    protected String id;
    @Field(type = FieldType.Boolean)
    protected Boolean deleted;
    @Version
    protected Long version;

//    **NOTE: deprecated at 2019-06-20
//    @Field(type = FieldType.Integer)
//    protected Integer eid;

    @Field(type = FieldType.Keyword)
    protected String label;
    @Field(type = FieldType.Keyword)
    protected String datasource;

    @Field(type = FieldType.Nested, includeInParent = true)
    protected Set<ElasticProperty> properties = new HashSet<>();

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
    @Override public void setProperties(Set<ElasticProperty> properties){
        this.properties = properties;
    }
    @Override public Set<ElasticProperty> getProperties(){ return this.properties; }

    /////////////////////////////////////////

    @Override
    public Optional<Object> getProperty(String key){
        List<ElasticProperty> result = properties.stream().filter(p->p.getKey().equals(key)).collect(Collectors.toList());
        if( result.size() == 0 ) return Optional.empty();

        Optional<Object> value = Optional.of(result.get(0).value()) ;
        return value;
    }

    @Override
    public Object getProperty(String key, Object defaultValue){
        List<ElasticProperty> result = properties.stream().filter(p->p.getKey().equals(key)).collect(Collectors.toList());
        if( result.size() == 0 ) return defaultValue;
        Optional<Object> value = Optional.of(result.get(0).value());
        return value.orElse(defaultValue);
    }

    @Override
    public ElasticProperty setProperty(String key, Object value) {
        return setProperty(key, value.getClass().getName(), value);
    }
    @Override
    public boolean setProperty(ElasticProperty property) {
        List<ElasticProperty> result = properties.stream().filter(p->p.getKey().equals(property.getKey())).collect(Collectors.toList());
        if( result.size() > 0 ){
            result.forEach(r->{
                properties.stream().filter(p->p.equals(r)).forEach(properties::remove);
            });
        }
        return properties.add(property);
    }

    @Override
    public ElasticProperty setProperty(String key, String type, Object value){
        List<ElasticProperty> result = properties.stream().filter(p->p.getKey().equals(key)).collect(Collectors.toList());
        if( result.size() > 0 ){
            result.forEach(r->{
                properties.stream().filter(p->p.equals(r)).forEach(properties::remove);
            });
        }
        ElasticProperty prop = new ElasticPropertyDocument(key, type, value);
        return properties.add(prop) ? prop : null;
    }

    @Override
    public boolean removeProperty(String key){
        List<ElasticProperty> result = properties.stream().filter(p->p.getKey().equals(key)).collect(Collectors.toList());
        if( result.size() > 0 ){
            result.forEach(r->{
                properties.stream().filter(p->p.equals(r)).forEach(properties::remove);
            });
        }
        return result.size() > 0;
    }

    @Override public boolean hasProperty(String key){
        List<ElasticProperty> result = properties.stream().filter(p->p.getKey().equals(key)).collect(Collectors.toList());
        return result.size() > 0;
    }

    @Override public void delete(){
        this.deleted = true;
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
