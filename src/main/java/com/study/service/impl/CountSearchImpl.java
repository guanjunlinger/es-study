package com.study.service.impl;

import com.study.service.CountSearchService;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class CountSearchImpl implements CountSearchService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public void countRequest(String indexName, String field, String content) {
        CountRequest countRequest = new CountRequest(indexName);
        countRequest.query(QueryBuilders.matchQuery(field, content));
        try {
            CountResponse countResponse = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);
            long count = countResponse.getCount();
            RestStatus restStatus = countResponse.status();
            System.out.println("count is :" + count + ", status is :" + restStatus.getStatus());
            int totalShards = countResponse.getTotalShards();
            System.out.println("totalShards is " + totalShards);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
