package net.bitnine.agenspop.util;

import net.bitnine.agenspop.config.properties.ProductProperties;
import org.springframework.http.HttpHeaders;

public final class AgensUtilHelper {

    public static final HttpHeaders productHeaders(ProductProperties productProperties){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", productProperties.getName());
        headers.add("agens.product.version", productProperties.getVersion());
        return headers;
    }

}
