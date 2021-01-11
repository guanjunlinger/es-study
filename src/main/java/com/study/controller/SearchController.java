package com.study.controller;

import com.study.service.SearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SearchController {

    @Autowired
    private SearchService searchService;

    @GetMapping("/es/search")
    public Object search(@RequestParam("field") String field, @RequestParam("keywords") String keywords) {
        return searchService.search(field, keywords);
    }
}