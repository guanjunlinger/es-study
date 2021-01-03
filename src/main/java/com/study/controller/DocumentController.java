package com.study.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.study.entity.Student;
import com.study.service.DocumentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class DocumentController {

    @Autowired
    private DocumentService indexService;

    private String indexName = "student";


    private ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping(value = "/put/document", consumes = "application/json")
    public Object putDocument(@RequestBody Student student) throws JsonProcessingException {
        indexService.putDocument(indexName, objectMapper.writeValueAsString(student));
        return "ok";
    }

    @GetMapping(value = "/get/document")
    public Object getDocument(@RequestParam("documentId") String documentId) {
        indexService.getDocument(indexName, documentId);
        return "ok";
    }

    @PostMapping(value = "/delete/document")
    public Object deleteDocument(@RequestParam("documentId") String documentId) {
        indexService.deleteDocument(indexName, documentId);
        return "ok";
    }

    @PostMapping(value = "/update/document/{documentId}")
    public Object updateDocument(@RequestBody Student student, @PathVariable String documentId) throws JsonProcessingException {
        indexService.updateDocument(indexName, documentId, objectMapper.writeValueAsString(student));
        return "ok";
    }

    @GetMapping(value = "/get/document/vector")
    public Object updateDocument(@RequestParam String documentId, @RequestParam("field") String field) throws JsonProcessingException {
        indexService.getTermVectors(indexName, documentId, field);
        return "ok";
    }
}
