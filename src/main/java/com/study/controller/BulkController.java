package com.study.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.study.entity.Student;
import com.study.service.BulkDocumentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
public class BulkController {

    private String indexName = "student";

    private ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private BulkDocumentService bulkDocumentService;

    @PostMapping(value = "/put/documents", consumes = "application/json")
    public Object putDocument(@RequestBody List<Student> students) throws JsonProcessingException {
        List<String> ids = new ArrayList<>();
        List<String> documents = new ArrayList<>();
        for (Student student : students) {
            ids.add(UUID.randomUUID().toString());
            documents.add(objectMapper.writeValueAsString(student));
        }
        bulkDocumentService.putDocuments(indexName, documents, ids);
        return "ok";
    }

    @PostMapping(value = "/put/bulk/document", consumes = "application/json")
    public Object putBulkDocument(@RequestBody List<Student> students) throws JsonProcessingException {
        List<String> ids = new ArrayList<>();
        List<String> documents = new ArrayList<>();
        for (Student student : students) {
            ids.add(UUID.randomUUID().toString());
            documents.add(objectMapper.writeValueAsString(student));
        }
        bulkDocumentService.putDocumentsByBulkProcessor(indexName, documents, ids);
        return "ok";
    }

    @GetMapping(value = "/bulk/get/document")
    public Object getDocument(@RequestParam("documentIds") List<String> documentIds) {
        bulkDocumentService.multiDocuments(indexName, documentIds);
        return "ok";
    }

    @GetMapping(value = "/bulk/get/term/vector")
    public Object getMultiTermVector(@RequestParam("documentIds") List<String> documentIds,
                                     @RequestParam("field") String field) {
        bulkDocumentService.multiTermVectors(indexName, documentIds, field);
        return "ok";
    }

}
