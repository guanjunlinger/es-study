package com.study.service.impl;

import com.study.service.DocumentService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;


@Service
public class DocumentServiceImpl implements DocumentService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    @Override
    public void putDocument(String indexName, String document) {
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.id("1");
        indexRequest.source(document, XContentType.JSON);
        restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new MyActionListener());
    }

    @Override
    public void getDocument(String indexName, String documentId) {
        GetRequest getRequest = new GetRequest(indexName, documentId);
        restHighLevelClient.getAsync(getRequest, RequestOptions.DEFAULT, new ResponseListener());
    }

    @Override
    public void deleteDocument(String indexName, String documentId) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, documentId);
        try {
            restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateDocument(String indexName, String documentId, String document) {
        UpdateRequest updateRequest = new UpdateRequest(indexName, documentId);
        updateRequest.doc(document, XContentType.JSON);
        try {
            restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getTermVectors(String indexName, String documentId, String field) {
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(indexName, documentId);
        termVectorsRequest.setFields(field);
        termVectorsRequest.setPositions(false);
        try {
            TermVectorsResponse termVectorsResponse = restHighLevelClient.termvectors(termVectorsRequest, RequestOptions.DEFAULT);
            for (TermVectorsResponse.TermVector termVector : termVectorsResponse.getTermVectorsList()) {
                   System.out.println(termVector.getFieldName()+"-> "+termVector.getFieldStatistics().getSumTotalTermFreq());

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    public static class MyActionListener implements ActionListener<IndexResponse> {


        @Override
        public void onResponse(IndexResponse indexResponse) {
            System.out.println(indexResponse);
        }

        @Override
        public void onFailure(Exception e) {
            System.out.println(e);
        }
    }

    public static class ResponseListener implements ActionListener<GetResponse> {


        @Override
        public void onResponse(GetResponse documentFields) {

            if (documentFields.isExists()) {
                System.out.println(documentFields.getSourceAsString());
            }
        }

        @Override
        public void onFailure(Exception e) {
            System.out.println(e);
        }
    }
}
