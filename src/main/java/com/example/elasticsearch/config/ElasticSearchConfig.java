package com.example.elasticsearch.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

@Configuration
@ComponentScan(basePackages = { "com.example.elasticsearch" })
public class ElasticSearchConfig {

    @Bean
    public RestClient getElasticSearchClient() {
        return RestClient.builder(
                new HttpHost("localhost", 9200, "http")).build();
    }
}
