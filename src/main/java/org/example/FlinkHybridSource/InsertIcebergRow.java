package org.example.FlinkHybridSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.example.other.Event2;
import org.example.serde.EventDeserializationSchemaKafka;

public class InsertIcebergRow {
    
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
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
                ";";
        tEnv.executeSql(createTable).print(); 

        tEnv.executeSql("INSERT INTO iceberg_catalog.default_database.table1 VALUES ('event4', '1006')").print();
        tEnv.executeSql("SELECT * FROM iceberg_catalog.default_database.table1").print();
        
    }
}
