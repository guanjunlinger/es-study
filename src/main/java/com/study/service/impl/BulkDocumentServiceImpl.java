package com.study.service.impl;

import com.study.service.BulkDocumentService;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class BulkDocumentServiceImpl implements BulkDocumentService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public void putDocuments(String indexName, List<String> document, List<String> documentsId) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < document.size(); i++) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            indexRequest.id(documentsId.get(i)).source(document.get(i), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        try {
            restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
