package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.shaded.org.checkerframework.checker.units.qual.t;
import org.apache.hadoop.conf.Configuration;
import org.example.other.Event2;
import org.example.serde.EventDeserializationSchemaKafka;
import java.util.HashMap;
import java.util.Map;

public class KafkaIcebergFlinkUnion {

    // Reads from Kafka topic and Iceberg table, then combines them into a single stream. Useful for "historical + real-time" use cases.
    //
    // Run:
    // - docker compose -f docker-compose-iceberg-rest.yml up
    // - docker compose -f docker-compose-kafka.yml up
    // - create Kafka topic: docker exec -ti kafka-processor bash -c "/opt/kafka/bin/kafka-topics.sh -bootstrap-server localhost:9092 --create --topic realtime-data-topic"
    // - produce Kafka event: docker exec -ti kafka-processor bash -c "/opt/kafka/bin/kafka-console-producer.sh -bootstrap-server localhost:9092 --topic realtime-data-topic"
    //   - payload: {"event_id": "event2", "user_id":"1002"}

    public static void main(String[] args) throws Exception {

        System.err.println("Starting Flink job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints-Union");
        env.setRestartStrategy(RestartStrategies.noRestart());

        //Iceberg
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String icebergCatalog = 
                "CREATE CATALOG iceberg_catalog WITH (\n" +
                "  'type' = 'iceberg',\n" +
                "  'catalog-type'='rest',\n" +
                // "  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',\n" +
                "  'uri' = 'http://localhost:8181',\n" +
                "  'warehouse' = 's3://warehouse/',\n" +
                "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
                "  's3.endpoint' = 'http://localhost:9000',\n" +
                "  's3.path-style-access' = 'true',\n" +
                "  'client.region' = 'us-east-1',\n" +
                "  's3.access-key-id' = 'admin',\n" +
                "  's3.secret-access-key' = 'password'\n" +
                ")";
        tEnv.executeSql(icebergCatalog).print();

        tEnv.executeSql("USE CATALOG iceberg_catalog").print();

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS default_database").print();
        tEnv.executeSql("SHOW DATABASES;").print();

        // tEnv.executeSql("DROP TABLE IF EXISTS default_database.table1").print();
        String createTable = "CREATE TABLE IF NOT EXISTS iceberg_catalog.default_database.table1 (\n" + 
                "  event_id STRING,\n" +
                "  user_id STRING\n" +
                ")\n" +
                "WITH (\n" +
                        "  'iceberg.catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',\n" +
                        "  'iceberg.uri' = 'http://rest:8181',\n" +
                        "  'iceberg.warehouse' = 's3://warehouse/',\n" +
                        "  'iceberg.io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
                        "  'iceberg.s3.endpoint' = 'http://minio:9002',\n" +
                        "  'iceberg.s3.path-style-access' = 'true',\n" +
                        "  'iceberg.client.region' = 'us-east-1',\n" +
                        "  'iceberg.s3.access-key-id' = 'admin',\n" +
                        "  'iceberg.s3.secret-access-key' = 'password'\n" +
                ");";
        tEnv.executeSql(createTable).print(); 

        tEnv.executeSql("INSERT INTO iceberg_catalog.default_database.table1 VALUES ('event1', '1003')").print();
        tEnv.executeSql("SELECT * FROM iceberg_catalog.default_database.table1").print();


        // // Iceberg - DataStream API configuration matching the SQL table
        // Map<String, String> catalogProperties = new HashMap<>();
        // catalogProperties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        // catalogProperties.put(CatalogProperties.URI, "http://rest:8181");
        // catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/");
        // catalogProperties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        // catalogProperties.put("s3.endpoint", "http://minio:9000");
        // catalogProperties.put("s3.path-style-access", "true");
        // catalogProperties.put("client.region", "us-east-1");
        // catalogProperties.put("s3.access-key-id", "admin");
        // catalogProperties.put("s3.secret-access-key", "password");
        
        // Configuration hadoopConfig = new Configuration();
        // TableLoader tableLoader = TableLoader.fromCatalog(
        //     new RESTCatalog(),
        //     TableIdentifier.of("default_catalog", "table1"),
        //     catalogProperties,
        //     hadoopConfig
        // );
        
        // IcebergSource<Event2> icebergSource = IcebergSource.forRowData()
        //     .tableLoader(tableLoader)
        //     .assignerFactory(new SimpleSplitAssignerFactory())
        //     .build();
        // DataStreamSource<Event2> icebergStream = env.fromSource(icebergSource, WatermarkStrategy.forMonotonousTimestamps(), "Iceberg Source");
        // icebergStream.print();


        // Kafka
        KafkaSource<Event2> kafkaSource = KafkaSource.<Event2>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("realtime-data-topic")
                .setGroupId("flink-consumer-group1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new EventDeserializationSchemaKafka())
                .build();

        DataStreamSource<Event2> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        kafkaStream.print();


        // // This tells Flink: "Read Iceberg completely, then switch to Kafka"
        // HybridSource<Event2> hybridSource = HybridSource.builder(icebergSource)
        //     .addSource(kafkaSource)
        //     .build();

        // // 4. Execute the Stream
        // DataStream<Event2> combinedStream = env.fromSource(
        //     hybridSource, 
        //     WatermarkStrategy.forMonotonousTimestamps(), 
        //     "Unified Sales Source"
        // );
        env.execute("Flink DataStream Kafka to Fluss Example");

    }
}
