package com.spark.iceberg.service;

import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import com.spark.iceberg.model.JournalEntry;

@Service
public class JournalService {
    private final SparkSession sparkSession;

    public JournalService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public void createTableIfNotExists(String tableName) {
        sparkSession.sql("CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "timestamp TIMESTAMP, " +
                "eventName STRING, " +
                "requestData MAP<STRING, STRING>, " +
                "userId STRING, " +
                "eventId STRING, " +
                "documentId STRING, " +
                "source MAP<STRING, STRING>, " +
                "labels ARRAY<STRING>" +
                ") USING iceberg");
    }

    public void addJournalEntry(String tableName, JournalEntry entry) {
        Dataset<Row> dataFrame = sparkSession.createDataFrame(Collections.singletonList(entry), JournalEntry.class);
        dataFrame.write().format("iceberg").mode("append").save(tableName);
    }

    public List<Row> getAllEntries(String tableName) {
        Dataset<Row> result = sparkSession.read().format("iceberg").load(tableName);
        return result.collectAsList();
    }
}
