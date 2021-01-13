package com.study.service.impl;

import com.study.service.IndexService;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CloseIndexResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class IndexServiceImpl implements IndexService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public void createIndex(String indexName, String shards, String replications, String mappings) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replications).build())
                .mapping(mappings, XContentType.JSON);

        try {
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            System.out.println("acknowledged is " + createIndexResponse.isAcknowledged());
            System.out.println("shardsAcknowledged is " + createIndexResponse.isShardsAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void deleteIndex(String indexName) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        try {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            System.out.println("acknowledged is " + acknowledgedResponse.isAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeIndex(String indexName) {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indexName);
        try {
            CloseIndexResponse closeIndexResponse = restHighLevelClient.indices().close(closeIndexRequest,RequestOptions.DEFAULT);
            System.out.println(closeIndexResponse.isAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void openIndex(String indexName) {
        OpenIndexRequest openIndexRequest =new OpenIndexRequest(indexName);
        try {
            OpenIndexResponse openIndexResponse = restHighLevelClient.indices().open(openIndexRequest,RequestOptions.DEFAULT);
            System.out.println(openIndexResponse.isAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
