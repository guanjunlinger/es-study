package com.study.service.impl;

import com.study.service.BulkDocumentService;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Iterator;
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
            BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                DocWriteResponse docWriteResponse = bulkItemResponse.getResponse();
                switch (bulkItemResponse.getOpType()) {
                    case INDEX:
                    case CREATE:
                        IndexResponse indexResponse = (IndexResponse) docWriteResponse;
                        String index = indexResponse.getIndex();
                        String id = indexResponse.getId();
                        long version = indexResponse.getVersion();
                        System.out.println(index + "->" + id + "->" + version);
                        break;
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void putDocumentsByBulkProcessor(String indexName, List<String> document, List<String> documentsId) {
        BulkProcessor bulkProcessor = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) -> restHighLevelClient
                .bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener), new MyBulkProcessorListener())
                .setBulkActions(500).setFlushInterval(TimeValue.timeValueSeconds(10L)).build();

        for (int i = 0; i < document.size(); i++) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            indexRequest.id(documentsId.get(i)).source(document.get(i), XContentType.JSON);
            bulkProcessor.add(indexRequest);
        }
    }

    @Override
    public void multiDocuments(String indexName, List<String> documentIds) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (String documents : documentIds) {
            multiGetRequest.add(new MultiGetRequest.Item(indexName, documents));
        }
        try {
            MultiGetResponse multiGetResponse = restHighLevelClient.mget(multiGetRequest, RequestOptions.DEFAULT);
            Iterator<MultiGetItemResponse> iterator = multiGetResponse.iterator();
            while (iterator.hasNext()) {
                MultiGetItemResponse multiGetItemResponse = iterator.next();
                System.out.println(multiGetItemResponse.getResponse().getSourceAsString());

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void multiTermVectors(String indexName, List<String> documentIds, String field) {
        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        for (String documentId : documentIds) {
            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(indexName, documentId);
            termVectorsRequest.setFields(field);
            multiTermVectorsRequest.add(termVectorsRequest);
        }
        try {
            MultiTermVectorsResponse multiTermVectorsResponse = restHighLevelClient.mtermvectors(multiTermVectorsRequest, RequestOptions.DEFAULT);
            for (TermVectorsResponse termVectorsResponse : multiTermVectorsResponse.getTermVectorsResponses()) {
                for (TermVectorsResponse.TermVector termVector : termVectorsResponse.getTermVectorsList()) {
                    System.out.println(termVectorsResponse.getIndex() + "->"
                            + termVectorsResponse.getId() + "->" + termVector.getTerms().get(0).getTotalTermFreq());

                }

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class MyBulkProcessorListener implements BulkProcessor.Listener {


        @Override
        public void beforeBulk(long executionId, BulkRequest bulkRequest) {
            System.out.println("开始 " + executionId + "请求数目:" + bulkRequest.numberOfActions());
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
            System.out.println("结束 " + executionId);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
            System.out.println("异常 " + executionId + " ," + throwable);
        }
    }
}
