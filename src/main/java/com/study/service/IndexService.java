package com.study.service;


import java.util.List;

public interface IndexService {
    void createIndex(String indexName,String shards,String replications,String mappings);

    void deleteIndex(String indexName);

    void closeIndex(String indexName);

    void openIndex(String indexName);

    void shrinkIndex(String sourceIndex ,String targetIndex);

    void refreshIndex(String ... indices);

    void forceMergeIndex(String ... indices);

    void createIndexAlias(String index,String alias);

    List<String> getIndexAlias(String index);
}
