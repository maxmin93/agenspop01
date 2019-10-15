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

    // **NOTE: Java Exception Handle in Stream Operations
    // https://kihoonkim.github.io/2017/09/09/java/noexception-in-stream-operations/

    public interface ExceptionSupplier<T> {
        T get() throws Exception;
    }

    public static <T> T wrapException(ExceptionSupplier<T> z) {
        try {
            return z.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
