package com.spark.iceberg.controller;

import java.util.List;

import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.spark.iceberg.model.JournalEntry;
import com.spark.iceberg.service.JournalService;

@RestController
@RequestMapping("/journal")
public class JournalController {

    private final JournalService journalService;

    @Autowired
    public JournalController(JournalService journalService) {
        this.journalService = journalService;
    }

    @PostMapping("/create")
    public void createTableIfNotExists(@RequestParam String tableName) {
        journalService.createTableIfNotExists(tableName);
    }

    @PostMapping("/add")
    public void addJournalEntry(@RequestBody JournalEntry journalEntry, @RequestParam String tableName) {
        journalService.addJournalEntry(tableName, journalEntry);
    }

    @GetMapping("/entries")
    public List<Row> getAllEntries(@RequestParam String tableName) {
        return journalService.getAllEntries(tableName);
    }

    // @GetMapping("/flattened-entries")
    // public List<Row> getFlattenedEntries(@RequestParam String tableName) {
    //     return journalService.getFlattenedEntries(tableName).collectAsList();
    // }
}
