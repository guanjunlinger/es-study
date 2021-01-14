package com.study.service.impl;

import com.study.service.IndexService;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CloseIndexResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            CloseIndexResponse closeIndexResponse = restHighLevelClient.indices().close(closeIndexRequest, RequestOptions.DEFAULT);
            System.out.println(closeIndexResponse.isAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void openIndex(String indexName) {
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
        try {
            OpenIndexResponse openIndexResponse = restHighLevelClient.indices().open(openIndexRequest, RequestOptions.DEFAULT);
            System.out.println(openIndexResponse.isAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shrinkIndex(String sourceIndex, String targetIndex) {
        ResizeRequest resizeRequest = new ResizeRequest(targetIndex, sourceIndex);
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", "1")
                .putNull("index.routing.allocation.require._name").build());
        try {
            ResizeResponse resizeResponse = restHighLevelClient.indices().shrink(resizeRequest, RequestOptions.DEFAULT);
            System.out.println("acknowledged is " + resizeResponse.isAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void refreshIndex(String... indices) {
        RefreshRequest refreshRequest = new RefreshRequest(indices);
        try {
            RefreshResponse refreshResponse = restHighLevelClient.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
            System.out.println(refreshResponse.getSuccessfulShards());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void forceMergeIndex(String... indices) {
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest(indices);
        try {
            ForceMergeResponse forceMergeResponse = restHighLevelClient.indices().forcemerge(forceMergeRequest, RequestOptions.DEFAULT);
            System.out.println(forceMergeResponse.getSuccessfulShards());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createIndexAlias(String index, String alias) {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(index).alias(alias));
        try {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
            System.out.println(acknowledgedResponse.isAcknowledged());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getIndexAlias(String index) {
        List<String> alias = new ArrayList<>();
        GetAliasesRequest aliasesRequest = new GetAliasesRequest(index);
        try {
            GetAliasesResponse getAliasesResponse = restHighLevelClient.indices().getAlias(aliasesRequest, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetadata>> map = getAliasesResponse.getAliases();
            for (Map.Entry<String, Set<AliasMetadata>> entry : map.entrySet()) {
                for (AliasMetadata aliasMetadata : entry.getValue()) {
                    alias.add(aliasMetadata.alias());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return alias;
    }
}
