package net.bitnine.agenspop.web;
import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.service.AgensSparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "${agens.api.base-path}/spark")
public class SparkController {

    private final AgensSparkService sparkService;
    private final ProductProperties productProperties;

    @Autowired
    public SparkController(AgensSparkService sparkService, ProductProperties productProperties){
        this.sparkService = sparkService;
        this.productProperties = productProperties;
    }

    ///////////////////////////////////////////

    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public String hello() throws Exception {
        sparkService.wordcount();
        return "{ \"msg\": \""+sparkService.hello()+"\"}";
    }

}
