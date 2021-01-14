package com.study.controller;

import com.study.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {

    @Autowired
    private IndexService indexService;

    @PostMapping("/es/search/put")
    public Object createIndex(@RequestParam("indexName") String indexName,
                              @RequestParam("shards") String shards,
                              @RequestParam("replications") String replications,
                              @RequestParam("mappings") String mappings) {

        indexService.createIndex(indexName, shards, replications, mappings);
        return "ok";
    }


    @PostMapping("/es/search/delete")
    public Object deleteIndex(@RequestParam("indexName") String indexName) {
        indexService.deleteIndex(indexName);
        return "ok";
    }

    @PostMapping("/es/search/open")
    public Object openIndex(@RequestParam("indexName") String indexName) {
        indexService.openIndex(indexName);
        return "ok";
    }

    @PostMapping("/es/search/close")
    public Object closeIndex(@RequestParam("indexName") String indexName) {
        indexService.closeIndex(indexName);
        return "ok";
    }

    @PostMapping("/es/search/shrink")
    public Object shrinkIndex(@RequestParam("sourceIndex") String sourceIndex,
                              @RequestParam("targetIndex") String targetIndex) {
        indexService.shrinkIndex(sourceIndex, targetIndex);
        return "ok";
    }

    @PostMapping("/es/search/flush")
    public Object flushIndex(@RequestParam("indexName") String indexName) {
        indexService.refreshIndex(indexName);
        return "ok";
    }

    @PostMapping("/es/search/merge")
    public Object mergeIndex(@RequestParam("indexName") String indexName) {
        indexService.forceMergeIndex(indexName);
        return "ok";
    }
    @PostMapping("/es/search/alias")
    public Object aliasIndex(@RequestParam("indexName") String indexName,
                             @RequestParam("alias") String alias) {
        indexService.createIndexAlias(indexName,alias);
        return "ok";
    }

    @GetMapping("/es/search/alias")
    public Object getAliasIndex(@RequestParam("indexName") String indexName) {
        return indexService.getIndexAlias(indexName);
    }


}
