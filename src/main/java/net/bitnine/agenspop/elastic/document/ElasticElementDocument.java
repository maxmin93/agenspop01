package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticEmptyProperty;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public abstract class ElasticElementDocument implements ElasticElement {

    public static final String DEFAULT_DATASOURCE = "default";

    @Id
    protected String id;
    @Field(type = FieldType.Boolean)
    protected Boolean deleted;
    @Version
    protected Long version;

    @Field(type = FieldType.Keyword)
    protected String datasource;
    @Field(type = FieldType.Long)
    protected Long eid;
    @Field(type = FieldType.Keyword)
    protected String label;

    @Field(type = FieldType.Nested, includeInParent = true)
    protected Set<ElasticPropertyDocument> properties = new HashSet<>();

    protected ElasticElementDocument(){
        this.deleted = false;
        this.version = System.currentTimeMillis();
    }
    protected ElasticElementDocument(Long eid, String label){
        this();
        this.datasource = DEFAULT_DATASOURCE;
        this.eid = eid;
        this.label = label;
        this.id = getId(eid, datasource);
    }
    protected ElasticElementDocument(Long eid, String label, String datasource) {
        this();
        this.datasource = datasource;
        this.eid = eid;
        this.label = label;
        this.id = getId(eid, datasource);
    }

    @Override public int hashCode() {
        return 31*eid.hashCode() + 43*datasource.hashCode();
    }
    @Override public boolean equals(final Object obj) {
        if (obj == null) return false;
        if (this.getClass() != obj.getClass()) return false;
        if (this == obj) return true;

        ElasticElement that = (ElasticElement) obj;
        if (this.eid == null || that.getEid() == null || !this.eid.equals(that.getEid()) )
            return false;
        if (this.datasource == null || that.getDatasource() == null || !this.datasource.equals(that.getDatasource()) )
            return false;

        return true;
    }

    @Override public String getId(){ return id; }
    public static final String getId(Long eid, String datasource){
        return datasource + "::" + eid.toString();
    }

    public Long getVersion(){ return version; }

    @Override public Long getEid(){ return eid; }
    @Override public String getLabel(){
        return label;
    }
    @Override public String getDatasource(){
        return datasource;
    }

    @Override public Iterable<String> getKeys(){
        return properties.stream().map(ElasticPropertyDocument::key).collect(Collectors.toList());
    }
    public void setProperties(Set<ElasticPropertyDocument> properties){ this.properties = properties; }
    public Set<ElasticPropertyDocument> getProperties(){ return this.properties; }


    @Override
    public ElasticProperty getProperty(String key){
        // return props.get(name);
        List<ElasticProperty> result = properties.stream().filter(p->p.key().equals(key)).collect(Collectors.toList());
        return result.size() > 0 ? result.get(0) : ElasticEmptyProperty.instance();
    }
    @Override
    public boolean setProperty(String key, Object value){
        List<ElasticProperty> result = properties.stream().filter(p->p.key().equals(key)).collect(Collectors.toList());
        if( result.size() > 0 ){
            result.forEach(r->{
                properties.stream().filter(p->p.equals(r)).forEach(properties::remove);
            });
        }
        ElasticPropertyDocument prop = new ElasticPropertyDocument(this.id, key, value);
        return properties.add(prop);
    }
    @Override
    public int removeProperty(String key){
        List<ElasticProperty> result = properties.stream().filter(p->p.key().equals(key)).collect(Collectors.toList());
        if( result.size() > 0 ){
            result.forEach(r->{
                properties.stream().filter(p->p.equals(r)).forEach(properties::remove);
            });
        }
        return result.size();
    }

    @Override public boolean hasProperty(String key){
        List<ElasticProperty> result = properties.stream().filter(p->p.key().equals(key)).collect(Collectors.toList());
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
        return "ElasticElementDocument{" +
                " id='" + id + '\'' +
                ", deleted=" + deleted +
                ", eid=" + eid +
                ", label='" + label + '\'' +
                ", datasource='" + datasource + '\'' +
                ", properties=[" + properties.stream().map(ElasticProperty::key).collect(Collectors.joining(",")) +
                "]}";
    }
}
