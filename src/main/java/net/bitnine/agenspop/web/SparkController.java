package net.bitnine.agenspop.web;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.config.properties.ProductProperties;
import net.bitnine.agenspop.dto.DataSetResult;
//import net.bitnine.agenspop.service.AgensGraphxService;
//import net.bitnine.agenspop.service.AgensSparkService;
import net.bitnine.agenspop.util.AgensUtilHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping(value = "${agens.api.base-path}/spark")
public class SparkController {

    // private final AgensSparkService sparkService;
    // private final AgensGraphxService graphxService;
    private final ProductProperties productProperties;

    @Autowired
    public SparkController(
            // AgensSparkService sparkService, AgensGraphxService graphxService,
            ProductProperties productProperties){
        // this.sparkService = sparkService;
        // this.graphxService = graphxService;
        this.productProperties = productProperties;
    }

    ///////////////////////////////////////////

//    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
//    @ResponseStatus(HttpStatus.OK)
//    public ResponseEntity<?> hello() throws Exception {
//        DataSetResult result = graphxService.readIndex();
//        return new ResponseEntity<>(result
//                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
//    }

    @GetMapping(value="/hello", produces="application/json; charset=UTF-8")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<?> hello() throws Exception {
        // sparkService.wordcount();
        return new ResponseEntity<>("Hello, spark!"
                , AgensUtilHelper.productHeaders(productProperties), HttpStatus.OK);
    }
}
