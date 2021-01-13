package com.study.service;


public interface IndexService {
    void createIndex(String indexName,String shards,String replications,String mappings);

    void deleteIndex(String indexName);

    void closeIndex(String indexName);

    void openIndex(String indexName);
}
