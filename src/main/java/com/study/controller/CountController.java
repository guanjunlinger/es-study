package com.study.controller;

import com.study.service.CountSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CountController {

    @Autowired
    private CountSearchService countSearch;

    @GetMapping("/es/search/count")
    public Object countSearch(@RequestParam("indexName") String indexName,
                                @RequestParam("field") String field,
                                @RequestParam("content") String content) {
        countSearch.countRequest(indexName, field, content);
        return "ok";
    }
}
