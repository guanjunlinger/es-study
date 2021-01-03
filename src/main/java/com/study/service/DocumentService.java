package com.study.service;

public interface DocumentService {
    void  putDocument(String indexName ,String document);

    void  getDocument(String indexName ,String documentId);

    void deleteDocument(String indexName,String documentId);

    void updateDocument(String indexName,String documentId,String document);

    void getTermVectors(String indexName,String documentId,String field);
}
