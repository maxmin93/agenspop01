package net.bitnine.agenspop.util;

import net.bitnine.agenspop.elasticgraph.model.ElasticVertex;
import net.bitnine.agenspop.elasticgraph.util.ElasticScrollIterator;
import net.bitnine.agenspop.service.AgensSparkService;

import java.util.List;

public class AgensSparkHelper {

    public static int putVertices(AgensSparkService service, ElasticScrollIterator<ElasticVertex> iterator ){
        int i=0;
        while( iterator.hasNext() ){
            List<ElasticVertex> slice = iterator.next();
            i += slice.size();
            //service.addVertices(slice);
        }

        return i;
    }
}
