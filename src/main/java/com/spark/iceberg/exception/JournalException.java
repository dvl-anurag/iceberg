package com.spark.iceberg.exception;

public class JournalException extends RuntimeException {
    public JournalException(String message, Throwable cause) {
        super(message, cause);
    }

    public JournalException(String message) {
        super(message);
    }
}