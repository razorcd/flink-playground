package org.example;/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.col;

//First start in terminal:  nc -lk 9001
public class StreamSet8KafkaSQL2 {

	public static void main(String[] args) throws Exception {

		Map<String, String> config = new HashMap<>();
		config.put("host", "localhost");
		config.put("port", "9001");

		// Set up Flink configuration to expose the Web UI
		Configuration flinkConfig = new Configuration();
		flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

		// Use LocalStreamEnvironment to enable the Flink Web UI
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
		final ParameterTool parameterTool = ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromMap(config));
		env.getConfig().setGlobalJobParameters(parameterTool);

//        {"user_id": 12345, "user_age": 30, "behavior": "purchase", "action_time": "2023-10-01T12:34:56.789"}
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String createTableSql =
                "CREATE TABLE Users (\n" +
                        "  user_id BIGINT,\n" +
                        "  action_time TIMESTAMP(3),\n" +
                        // Define Event Time and Watermark Strategy
                        "  WATERMARK FOR action_time AS action_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'test2',\n" +
                        "  'properties.bootstrap.servers' = 'localhost:9092',\n" + // Replace with your Kafka address
                        "  'properties.group.id' = 'flink-consumer-group',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" + // Assuming the Kafka messages are JSON
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")";
        tEnv.executeSql(createTableSql).print();
//        TableEnvironment tEnv = TableEnvironment.create(flinkConfig);



        String selectSqlWindowed =
                "SELECT\n" +
                "  TUMBLE_START(action_time, INTERVAL '10' SECOND) AS window_start,\n" +
                "  TUMBLE_END(action_time, INTERVAL '10' SECOND) AS window_end,\n" +
                "  -- If LAST_VALUE returns NULL, replace it with 0\n" +
                "  COALESCE(LAST_VALUE(user_id), 0) AS last_user_id_in_window\n" +
                "FROM\n" +
                "  TABLE(TUMBLE(TABLE Users, DESCRIPTOR(action_time), INTERVAL '10' SECOND))\n" +
                "GROUP BY\n" +
                "  window_start, window_end;"
                ;

        String selectSql =
                        "SELECT\n" +
                        "  user_id,\n" +
                        "  action_time\n" +
                        "FROM Users\n"
//                        "WHERE behavior = 'purchase'"
                    ;


        Table resultTable = tEnv.sqlQuery(selectSqlWindowed);

//        resultTable.window(Timer.of(Time.of(5, TimeUnit.SECONDS)))

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
