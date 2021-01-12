package com.study.service;

import java.util.List;

public interface SearchService {
    List<String> searchFieldCapability(String field, List<String> value);

    List<String> search(String field, String value);

    List<String> scrollSearch(String indexName, String field, String value);

    String explainRequest(String indexName,String documentId,String field,String content);

}
