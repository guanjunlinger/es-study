package com.study.service;

import java.util.List;

public interface BulkDocumentService {

    void putDocuments(String indexName, List<String> document, List<String> documentsId);

    void putDocumentsByBulkProcessor(String indexName, List<String> document, List<String> documentsId);

    void multiDocuments(String indexName ,List<String> documents);

    void  multiTermVectors(String indexName,List<String> documentIds,String field);
}
