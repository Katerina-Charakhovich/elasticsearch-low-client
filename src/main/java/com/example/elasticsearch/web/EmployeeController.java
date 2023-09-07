package com.example.elasticsearch.web;

import com.example.elasticsearch.model.Employee;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping(path = "/employees", produces = "application/json")
public class EmployeeController {

    @Autowired
    RestClient restClient;

    @Autowired
    ObjectMapper objectMapper;

    private static final String INDEX_SEARCH = "employees/_search";
    private static final String INDEX_DOC = "employees/_doc/";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";


    @GetMapping
    public ResponseEntity<?> findAllEmployees() {
        try {
            Response response = restClient.performRequest(new Request(METHOD_GET, INDEX_SEARCH));
            String responseBody = EntityUtils.toString(response.getEntity());
            List<Employee> employees = convertToListEmployee(responseBody);
            return ResponseEntity.ok(employees);
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> findEmployeeById(@PathVariable final String id) {
        try {
            Response response = restClient.performRequest(new Request(METHOD_GET, INDEX_DOC + id));
            String responseBody = EntityUtils.toString(response.getEntity());
            Employee employee = convertToEmployee(responseBody);
            return ResponseEntity.ok(employee);
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody final Employee employee) {
        try {
            if (employee.getId() == null) {
                employee.setId(UUID.randomUUID().toString());
            }
            Request request = new Request(METHOD_PUT,INDEX_DOC + employee.getId());
            request.setJsonEntity(objectMapper.writeValueAsString(employee));
            Response response = restClient.performRequest(request);
            return ResponseEntity.ok().build();
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteById(@PathVariable final String id) {
        try {
            Response response = restClient.performRequest(new Request(METHOD_DELETE, INDEX_DOC + id));
            int statusCode = response.getStatusLine().getStatusCode();

            return statusCode == 200
                    ? ResponseEntity.ok().build()
                    : ResponseEntity.badRequest().build();
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/search")
    public ResponseEntity<?> findBySearchParams(@RequestParam(name = "field") String field,
                                                @RequestParam String value) {
        try {
            String query = "{\"query\":{\"match\":{\"" + field + "\":\"" + value + "\"}}}";
            Request request = new Request(METHOD_GET, INDEX_SEARCH);
            request.setJsonEntity(query);
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            List<Employee> employees = convertToListEmployee(responseBody);
            return ResponseEntity.ok(employees);
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/aggregation")
    public ResponseEntity<?> findBySearchParams(@RequestParam(name = "field") String field,
                                                @RequestParam(name = "metricType") String metricType,
                                                @RequestParam(name = "metricField") String metricField) {
        try {
            String query = "{\"size\":0, " +
                    "\"aggs\":{\"agg-by-skill\":{\"terms\":{\"field\":\"" + field + "\"}," +
                    "\"aggs\":{\"metric\":{\"" + metricType + "\":{\"field\":\"" + metricField + "\"}}}}}}";
            Request request = new Request(METHOD_GET, INDEX_SEARCH);
            request.setJsonEntity(query);
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            return ResponseEntity.ok(responseBody);
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    private List<Employee> convertToListEmployee(String json) throws JsonProcessingException {
        List<Employee> employees = new ArrayList<>();
        final JsonNode node = objectMapper.readTree(json);
        for (Iterator<JsonNode> it = node.get("hits").get("hits").elements(); it.hasNext(); ) {
            JsonNode hitNode = it.next();
            Employee employee = objectMapper.readValue(hitNode.get("_source").toString(), Employee.class);
            String employeeId = hitNode.get("_id").toString();
            employee.setId(employeeId);
            employees.add(employee);
        }
        return employees;
    }

    private Employee convertToEmployee(String json) throws JsonProcessingException {
        final JsonNode node = objectMapper.readTree(json);
        return  objectMapper.readValue(node.get("_source").toString(), Employee.class);
    }
}
