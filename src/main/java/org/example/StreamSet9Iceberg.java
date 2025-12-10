package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

//First start in terminal:  nc -lk 9001
public class StreamSet9Iceberg {

        public static void main(String[] args) throws Exception {

            Configuration hadoopConf = new Configuration();
//            hadoopConf.set("hive.metastore.uris", "thrift://localhost:9083");


            TableLoader tableLoader = TableLoader.fromCatalog(null, null);
//            TableLoader tableLoader = TableLoader.fromCatalog(
//                    hadoopConf,
//                    "hive_catalog", // Catalog Name
//                    "nyc.taxis" // Full table identifier
//            );

            DataStream<RowData> sourceStream = FlinkSource.forRowData()
                    .tableLoader(tableLoader)
                    .streaming(true)
                    .build();

            sourceStream.print();

//// Example Write:
//            FlinkSink.forRowData(outputStream)
//                    .tableLoader(tableLoader)
//                    .append();



            EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
            TableEnvironment tEnv = TableEnvironment.create(settings);

//            Hive SQL client:
//            DESCRIBE FORMATTED nyc.taxis;
            String catalogSql = String.join("\n",
                    "CREATE CATALOG hive_catalog WITH (",
                    "  'type'='iceberg',",
                    "  'catalog-type'='hive',",
                    "  'uri'='thrift://localhost:9083',",
                    "  'warehouse'='file:/opt/hive/data/warehouse'",
                    ");"
            );

            tEnv.executeSql(catalogSql);

            tEnv.executeSql("USE CATALOG hive_catalog;");
            tEnv.executeSql("SELECT * FROM my_iceberg_table;");
//
////        System.setProperty("fs.s3a.endpoint", "http://storage:9000");
//            System.setProperty("fs.s3a.endpoint", "http://localhost:9001");
//            System.setProperty("fs.s3a.access.key", "admin");
//            System.setProperty("fs.s3a.secret.key", "password");
//            System.setProperty("fs.s3a.path.style.access", "true");
//
//            // set up the execution environment
//            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//
//            // set up the table environment
//            final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
//                    env,
//                    EnvironmentSettings.newInstance().inStreamingMode().build());
//
//            // create the Nessie catalog
//            tableEnv.executeSql(
//                    "CREATE CATALOG iceberg WITH ("
//                            + "'type'='iceberg',"
//                            + "'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
//                            + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
//                            + "'uri'='http://localhost:19120/api/v1',"
//                            + "'authentication.type'='none',"
//                            + "'ref'='main',"
//                            + "'client.assume-role.region'='us-east-1',"
//                            + "'warehouse' = 's3://warehouse',"
//                            + "'s3.endpoint'='http://localhost:9002',"
//                            + "'s3.access.key'='admin',"
//                            + "'s3.secret.key'='password'"
//                            + ")");
//
//            // List all catalogs
//            TableResult result = tableEnv.executeSql("SHOW CATALOGS");
//
//            // Print the result to standard out
//            result.print();
//
//
//
//






//
//		Map<String, String> config = new HashMap<>();
//		config.put("host", "localhost");
//		config.put("port", "9001");
//
//		// Set up Flink configuration to expose the Web UI
//		Configuration flinkConfig = new Configuration();
//		flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//
//		// Use LocalStreamEnvironment to enable the Flink Web UI
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
//		final ParameterTool parameterTool = ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromMap(config));
//		env.getConfig().setGlobalJobParameters(parameterTool);
//
//
//		// KafkaSource (new API)
//		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//				.setBootstrapServers("localhost:9092")
//				.setTopics("test1")
//				.setGroupId("flink-group-1")
//				.setStartingOffsets(OffsetsInitializer.latest())
//				.setValueOnlyDeserializer(new SimpleStringSchema())
//				.build();
//
//		// Add Kafka source using new API
//		DataStreamSource<String> kafkaStreamSource = env.fromSource(
//				kafkaSource,
//				WatermarkStrategy.noWatermarks(),
//				"Kafka-Source"
//		);
//
//		DataStream<Tuple3<Integer, String, String>> kafkaStream = kafkaStreamSource
//				.map(line -> {
//						String[] parts = line.split(",");
//						Integer col1 = Integer.parseInt(parts[0]);
//						String col2 = parts[1];
//						String col3 = parts[2];
//						return new Tuple3<>(col1, col2, col3);
//					})
//				.returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));
//
//
//		Schema schema = Schema.newBuilder()
////				.primaryKey("f0")
//				.column("column1", "INT")
//				.column("column2", "STRING")
//				.column("column3", "STRING")
//				.build();
//
//		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
////		tableEnv.registerDataStream("KafkaSource", kafkaStream);
//
//		Table t1 = tableEnv.fromDataStream(kafkaStream)
//				.select($("*"))
//				.where($("f1").isEqual("A"))
//				;
////		Table t1 = tableEnv.sqlQuery("SELECT * FROM KafkaSource WHERE f1 = 'A'");
//
//		t1.execute().print();
	}
}
