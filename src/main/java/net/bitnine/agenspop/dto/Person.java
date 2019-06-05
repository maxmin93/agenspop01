package net.bitnine.agenspop.dto;

import org.json.simple.JSONObject;

import javax.validation.constraints.NotEmpty;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@SuppressWarnings("unchecked")
public class Person {

    @NotEmpty(message = "ID must be not empty.")
    private int id;
    private String name;
    private String email;
    private int age;
    private LocalDate registeredDate;
    private BigDecimal salary;

    public Person() {
    }

    public Person(@NotEmpty(message = "ID must be not empty.") int id, String name
            , String email, int age, LocalDate registeredDate, BigDecimal salary) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.age = age;
        this.registeredDate = registeredDate;
        this.salary = salary;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Person ").append(toJson().toJSONString());
        return sb.toString();
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("name", name);
        json.put("email", email);
        json.put("age", age);
        json.put("registered-date", registeredDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        json.put("salary", salary.longValue());

        return json;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public LocalDate getRegisteredDate() {
        return registeredDate;
    }

    public void setRegisteredDate(LocalDate registeredDate) {
        this.registeredDate = registeredDate;
    }

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }
}