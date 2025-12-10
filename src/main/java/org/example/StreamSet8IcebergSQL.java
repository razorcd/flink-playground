package org.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;

//First start in terminal:  nc -lk 9001
public class StreamSet8IcebergSQL {

	public static void main(String[] args) throws Exception {

		Map<String, String> config = new HashMap<>();
		config.put("host", "localhost");
		config.put("port", "9001");

		// Set up Flink configuration to expose the Web UI
//		Configuration flinkConfig = new Configuration();
//		flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		// Use LocalStreamEnvironment to enable the Flink Web UI
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);


		final ParameterTool parameterTool = ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromMap(config));
		env.getConfig().setGlobalJobParameters(parameterTool);

//        {"user_id": 12345, "user_age": 30, "behavior": "purchase", "action_time": "2023-10-01T12:34:56.789"}
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String createTableSql =
                "CREATE CATALOG iceberg WITH (\n" +
                "    'type' = 'iceberg',\n" +
                "    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',\n" +
                "    'uri' = 'http://localhost:8181',                       \n" +
                "    'warehouse' = 's3://warehouse/',                       \n" +
                "    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',      \n" +
                "    's3.endpoint' = 'http://localhost:9002',               \n" +
                "    's3.path-style-access' = 'true',                       \n" +
                "    'client.region' = 'us-east-1',                         \n" +
                "    's3.access-key-id' = 'admin',                          \n" +
                "    's3.secret-access-key' = 'password'                    \n" +
                ");";
        tEnv.executeSql(createTableSql).print();





//                "CREATE TABLE Users (\n" +
//                        "  user_id BIGINT,\n" +
//                        "  action_time TIMESTAMP(3),\n" +
//                        // Define Event Time and Watermark Strategy
//                        "  WATERMARK FOR action_time AS action_time - INTERVAL '5' SECOND\n" +
//                        ") WITH (\n" +
//                        "  'connector' = 'kafka',\n" +
//                        "  'topic' = 'test2',\n" +
//                        "  'properties.bootstrap.servers' = 'localhost:9092',\n" + // Replace with your Kafka address
//                        "  'properties.group.id' = 'flink-consumer-group',\n" +
//                        "  'scan.startup.mode' = 'earliest-offset',\n" +
//                        "  'format' = 'json',\n" + // Assuming the Kafka messages are JSON
//                        "  'json.ignore-parse-errors' = 'true'\n" +
//                        ")";
//        TableEnvironment tEnv = TableEnvironment.create(flinkConfig);

//
//
//        String selectSqlWindowed =
//                "SELECT\n" +
//                "  TUMBLE_START(action_time, INTERVAL '10' SECOND) AS window_start,\n" +
//                "  TUMBLE_END(action_time, INTERVAL '10' SECOND) AS window_end,\n" +
//                "  -- If LAST_VALUE returns NULL, replace it with 0\n" +
//                "  COALESCE(LAST_VALUE(user_id), 0) AS last_user_id_in_window\n" +
//                "FROM\n" +
//                "  TABLE(TUMBLE(TABLE Users, DESCRIPTOR(action_time), INTERVAL '10' SECOND))\n" +
//                "GROUP BY\n" +
//                "  window_start, window_end;"
//                ;
//
//        String selectSql =
//                        "SELECT\n" +
//                        "  user_id,\n" +
//                        "  action_time\n" +
//                        "FROM Users\n"
////                        "WHERE behavior = 'purchase'"
//                    ;
//
//

        String selectSqlWindowed =
                "SELECT * FROM iceberg.db.rawdatastream_sink;"
                ;

        Table resultTable = tEnv.sqlQuery(selectSqlWindowed);
//
////        resultTable.window(Timer.of(Time.of(5, TimeUnit.SECONDS)))
//
        resultTable.execute().print();



//
//
//		// KafkaSource (new API)
//		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//				.setBootstrapServers("localhost:9092")
//				.setTopics("test2")
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
//
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
