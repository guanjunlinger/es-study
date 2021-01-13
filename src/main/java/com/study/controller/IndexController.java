package com.study.controller;

import com.study.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
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

}
