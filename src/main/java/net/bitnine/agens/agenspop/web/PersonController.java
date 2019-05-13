package net.bitnine.agens.agenspop.web;

import net.bitnine.agens.agenspop.dto.Person;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "${agens.api.base-path}/person")
public class PersonController {

    List<Person> personRepository;

    @Autowired
    public PersonController(){
    }

    @PostConstruct
    public void init() {
        // @formatter:off
        personRepository = new ArrayList<>(Arrays.asList(
                new Person(1, "Jhon", "jhon@test.com", 20, LocalDate.of(2019, 9, 9), BigDecimal.valueOf(1000)),
                new Person(2, "Jhon", "jhon1@test.com", 20, LocalDate.of(2019, 9, 9), BigDecimal.valueOf(1500)),
                new Person(3, "Jhon", null, 20, LocalDate.of(2019, 9, 9), BigDecimal.valueOf(1000)),
                new Person(4, "Tom", "tom@test.com", 21, LocalDate.of(2019, 9, 9), BigDecimal.valueOf(1500)),
                new Person(5, "Mark", "mark@test.com", 21, LocalDate.of(2019, 9, 9), BigDecimal.valueOf(1200)),
                new Person(6, "Julia", "jhon@test.com", 20, LocalDate.of(2019, 9, 9), BigDecimal.valueOf(1000))));
        // @formatter:on
    }

    @GetMapping("/{id}")
    public Person findById(@PathVariable final int id) throws Exception {
        try {
            return personRepository.get(id);
        }catch (Exception ex){
            // Whitelabel Error Page
            throw new ResponseStatusException(HttpStatus.NOT_FOUND
                    , String.format("Person(%d) Not Found", id), ex);
        }
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public boolean insertPerson(@RequestBody final Person person) {
        Objects.requireNonNull(person);
        return personRepository.add(person);
    }

    @GetMapping
    public List<Object> findAll() {

        return personRepository.stream().map(p -> (Object)p.toJson()).collect(Collectors.toList());
    }


/*
https://www.baeldung.com/spring-response-status-exception
    ResponseStatusException(HttpStatus status)
    ResponseStatusException(HttpStatus status, java.lang.String reason)
    ResponseStatusException(
      HttpStatus status,
      java.lang.String reason,
      java.lang.Throwable cause
    )
 */

    @GetMapping("/error1")
    public void testError1(){
        throw new IllegalArgumentException("testError1");
    }

    @GetMapping("/error2")
    public void testError2() throws Exception {
        throw new IllegalAccessException("testError2");
    }

}