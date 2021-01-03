package com.study.service;

import java.util.List;

public interface BulkDocumentService {

    void putDocuments(String indexName, List<String> document, List<String> documentsId);
}
