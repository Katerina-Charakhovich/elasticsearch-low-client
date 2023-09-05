package com.example.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Employee {
    private String id;
    private String name;
    private String email;
    private List<String> skills;
    private Integer experience;
    private String rating;
    private String description;
    private boolean verified;
    private Integer salary;
    private Address address;
}
