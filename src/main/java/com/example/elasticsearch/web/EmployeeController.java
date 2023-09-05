package com.example.elasticsearch.web;

import com.example.elasticsearch.model.Employee;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping(path ="/employees", produces="application/json" )
public class EmployeeController {

    @Autowired
    RestClient restClient;

    @Autowired
    ObjectMapper objectMapper;

    @GetMapping
    public ResponseEntity<?> findAllEmployees() {
        try {
            Response response = restClient.performRequest(new Request("GET", "employees/_search"));
            String responseBody = EntityUtils.toString(response.getEntity());
            return ResponseEntity.ok(responseBody);
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> findEmployeeById(@PathVariable final String id) throws IOException {
        try {
            Response response = restClient.performRequest(new Request("GET", "employees/_doc/" + id));
            String responseBody = EntityUtils.toString(response.getEntity());
            final JsonNode node = objectMapper.readTree(responseBody);
            Employee employee = objectMapper.readValue(node.get("_source").toString(), Employee.class);
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
                return ResponseEntity.badRequest().build();
            }
            Request request = new Request(
                    "PUT",
                    "employees/_doc/" + employee.getId());
            request.setJsonEntity(objectMapper.writeValueAsString(employee));
            restClient.performRequest(request);
            return ResponseEntity.ok().build();
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteById(@PathVariable final String id) {
        try {
            Response response = restClient.performRequest(new Request("DELETE", "employees/_doc/" + id));
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
            int i = 0;
            String query = "{\"query\":{\"match\":{\"" +field+"\":\"" + value + "\"}}}";
            Request request = new Request("GET", "employees/_search");
            request.setJsonEntity(query);
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            final JsonNode node = objectMapper.readTree(responseBody);
            var employees = objectMapper.readValue(node.get("_source").toString(), Employee.class);
                   // objectMapper.getTypeFactory().constructCollectionType(List.class, Employee.class));
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
                    "\"aggs\":{\"agg-by-skill\":{\"terms\":{\"field\":\"" +field+"\"}," +
                    "\"aggs\":{\"metric\":{\""+metricType+"\":{\"field\":\""+metricField+"\"}}}}}}";
            Request request = new Request("GET", "employees/_search");
            request.setJsonEntity(query);
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            return ResponseEntity.ok(responseBody);
        } catch (IOException ex) {
            return ResponseEntity.badRequest().build();
        }
    }
}
