package com.study.service.impl;

import com.study.service.SearchService;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class SearchServiceImpl implements SearchService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public List<String> searchFieldCapability(String field, List<String> value) {
        List<String> result = new ArrayList<>();
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
                .fields(field)
                .indices(value.toArray(new String[0]))
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        try {
            FieldCapabilitiesResponse response = restHighLevelClient.fieldCaps(request, RequestOptions.DEFAULT);
            Map<String, FieldCapabilities> fieldCapabilitiesMap = response.getField(field);
            FieldCapabilities textCapabilities = fieldCapabilitiesMap.get("text");
            System.out.println("isSearchable is " + textCapabilities.isSearchable());
            System.out.println("isAggregatable is " + textCapabilities.isAggregatable());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;

    }

    @Override
    public List<String> search(String field, String value) {
        List<String> result = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(10).timeout(TimeValue.timeValueSeconds(60));

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, value)
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(5)
                .maxExpansions(10);
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits searchHits = searchResponse.getHits();
            for (SearchHit searchHit : searchHits) {
                result.add(searchHit.getSourceAsString());

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public List<String> scrollSearch(String indexName, String field, String value) {
        List<String> result = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(2);
        searchSourceBuilder.query(QueryBuilders.matchQuery(field, value));
        searchRequest.source(searchSourceBuilder);

        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHits searchHits = searchResponse.getHits();
            while (Objects.nonNull(searchHits) && searchHits.getHits().length > 0) {
                for (SearchHit searchHit : searchHits) {
                    result.add(searchHit.getSourceAsString());
                }
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scroll(TimeValue.timeValueSeconds(30L));
                searchResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                searchHits = searchResponse.getHits();
                scrollId = searchResponse.getScrollId();
                System.out.println("scroll id is :" + scrollId);
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public String explainRequest(String indexName, String documentId, String field, String content) {
        ExplainRequest explainRequest = new ExplainRequest(indexName, documentId);
        explainRequest.query(QueryBuilders.termQuery(field, content));
        try {
            ExplainResponse explainResponse = restHighLevelClient.explain(explainRequest, RequestOptions.DEFAULT);
            Explanation explanation = explainResponse.getExplanation();
            return "" + explanation;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
