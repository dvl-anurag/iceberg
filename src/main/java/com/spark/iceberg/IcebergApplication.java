package com.spark.iceberg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

public class IcebergApplication {

    public static void main(String[] args) throws IOException {
        // Initialize Spark Session with PostgreSQL catalog for Iceberg
        SparkSession spark = SparkSession.builder()
                .appName("IcebergCRUDApp")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") // Iceberg SparkCatalog
                .config("spark.sql.catalog.spark_catalog.type", "postgres") // PostgreSQL as the catalog type
                .config("spark.sql.catalog.spark_catalog.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog") // PostgreSQL URL
                .config("spark.sql.catalog.spark_catalog.driver", "org.postgresql.Driver") // PostgreSQL Driver
                .config("spark.sql.catalog.spark_catalog.user", "postgres") // PostgreSQL Username
                .config("spark.sql.catalog.spark_catalog.password", "root") // PostgreSQL Password
                .config("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.catalog.PostgresCatalog") // Catalog impl for PostgreSQL
                .config("spark.sql.catalog.spark_catalog.warehouse", "file:///C:/spar") // Warehouse location for Iceberg data
                .getOrCreate();

        // Define schema for the journal entry (with dynamic fields as Map)
        StructType journalEntrySchema = new StructType()
                .add("timestamp", DataTypes.TimestampType)
                .add("eventName", DataTypes.StringType)
                .add("requestData", new MapType(DataTypes.StringType, DataTypes.StringType, true)) // Dynamic JSON as Map
                .add("userId", DataTypes.StringType)
                .add("eventId", DataTypes.StringType)
                .add("documentId", DataTypes.StringType)
                .add("source", new MapType(DataTypes.StringType, DataTypes.StringType, true)) // Dynamic JSON as Map
                .add("labels", DataTypes.createArrayType(DataTypes.StringType));

        // Read JSON data into DataFrame using the schema
        Dataset<Row> df = spark.read().schema(journalEntrySchema).json("F:/work/iceberg/spark-warehouse/json_file.json");

        // Flatten the 'requestData' Map and 'source' Map (optional)
        df = df.withColumn("requestData_flat", functions.col("requestData"))
               .withColumn("source_flat", functions.col("source"));

        // Display schema and sample data
        df.printSchema();
        df.show();

        // Perform the cleanup logic before writing data
        cleanUpIcebergMetadataDirectory("C:/spar/default/journal");

        // Write processed DataFrame to Iceberg table in PostgreSQL catalog
        df.write()
          .format("iceberg")
          .mode("overwrite")
          .save("spark_catalog.default.journal");

        spark.stop();
    }

    /**
     * Cleans up the metadata directory to ensure that Iceberg can create fresh files.
     *
     * @param metadataDirPath The path to the metadata directory.
     * @throws IOException If an I/O error occurs while interacting with the file system.
     */
    private static void cleanUpIcebergMetadataDirectory(String metadataDirPath) throws IOException {
        Configuration hadoopConf = new Configuration();
        FileSystem fs = FileSystem.get(hadoopConf);

        // Path to the metadata directory
        Path metadataPath = new Path(metadataDirPath + "/metadata");

        // Check if the directory exists
        if (fs.exists(metadataPath)) {
            // Delete the directory and all its contents recursively
            fs.delete(metadataPath, true); // true means delete recursively
            System.out.println("Deleted existing metadata directory: " + metadataPath);
        }

        // Ensure the directory is recreated for Iceberg to use
        if (!fs.exists(metadataPath)) {
            fs.mkdirs(metadataPath);
            System.out.println("Created new metadata directory: " + metadataPath);
        }
    }
}
