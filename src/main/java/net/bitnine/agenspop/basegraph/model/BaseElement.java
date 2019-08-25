package net.bitnine.agenspop.basegraph.model;

import java.util.Collection;
import java.util.List;

public interface BaseElement {

    String getId();
    String getLabel();
    String getDatasource();

    List<String> keys();
    List<Object> values();

    boolean exists(String key);

    Collection<BaseProperty> properties();
    void properties(Collection<? extends BaseProperty> properties);

    BaseProperty getProperty(String key);
    BaseProperty getProperty(String key, BaseProperty defaultProperty);

    // upsert
    void setProperty(BaseProperty property);
    // delete
    BaseProperty removeProperty(String key);

}
