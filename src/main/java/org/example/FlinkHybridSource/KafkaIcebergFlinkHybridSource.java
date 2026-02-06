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

public class KafkaIcebergFlinkHybridSource {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints-Union");
        env.setRestartStrategy(RestartStrategies.noRestart());

      // REST Catalog properties
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        catalogProperties.put(CatalogProperties.URI, "http://localhost:8181");
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/");
        catalogProperties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProperties.put("s3.endpoint", "http://localhost:9000");
        catalogProperties.put("s3.path-style-access", "true");
        catalogProperties.put("client.region", "us-east-1");
        catalogProperties.put("s3.access-key-id", "admin");
        catalogProperties.put("s3.secret-access-key", "password");
        
        // CatalogLoader and TableLoader
        CatalogLoader catalogLoader = CatalogLoader.custom(
            "rest_catalog",
            catalogProperties,
            new org.apache.hadoop.conf.Configuration(),
            "org.apache.iceberg.rest.RESTCatalog"
        );
        
        TableLoader tableLoader = TableLoader.fromCatalog(
            catalogLoader,
            TableIdentifier.of("default_database", "table1")
        );


        // IcebergSource for streaming
        IcebergSource<Event2> icebergSource = IcebergSource.forOutputType(new Event2RawDataConvertor())
            .tableLoader(tableLoader)
            .assignerFactory(new SimpleSplitAssignerFactory())
            // .streaming(true)
            // .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
            // .monitorInterval(Duration.ofSeconds(60)) // Poll every 60s
            .build();

        // DataStream<Event2> stream = env.fromSource(
        //     icebergSource,
        //     WatermarkStrategy.noWatermarks(),
        //     "IcebergSource",
        //     TypeInformation.of(Event2.class)
        // );
        // stream.print();


        // Kafka
        KafkaSource<Event2> kafkaSource = KafkaSource.<Event2>builder() //todo: identify how ensure the commited events are immediate
                .setBootstrapServers("localhost:9092")
                .setTopics("realtime-data-topic")
                .setGroupId("flink-consumer-group1")
                // .setProperty("auto.commit.interval.ms", "10")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new EventDeserializationSchemaKafka())
                .build();

        // DataStreamSource<Event2> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        // kafkaStream.print();


        // This tells Flink: "Read Iceberg completely, then switch to Kafka"
        HybridSource<Event2> hybridSource = HybridSource.builder(icebergSource)
            .addSource(kafkaSource)
            .build();

        DataStream<Event2> combinedStream = env.fromSource(
            hybridSource, 
            WatermarkStrategy.forMonotonousTimestamps(), 
            "Unified Sales Source"
        )
        .returns(TypeInformation.of(Event2.class));
        combinedStream.print(); 

        env.execute("Flink job");

    }
}
