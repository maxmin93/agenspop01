package net.bitnine.agenspop.basegraph.model;

import java.util.Collection;
import java.util.List;

public interface BaseElement {

    String getId();
    String getLabel();
    String getDatasource();

    boolean notexists();
    void remove();

    Collection<String> keys();
    Collection<Object> values();

    Collection<BaseProperty> properties();
    void properties(Collection<? extends BaseProperty> properties);

    boolean hasProperty(String key);
    BaseProperty getProperty(String key);
    BaseProperty getProperty(String key, BaseProperty defaultProperty);

    // upsert
    void setProperty(BaseProperty property);
    // delete
    BaseProperty removeProperty(String key);

}
