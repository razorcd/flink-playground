/*
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

package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.example.other.CustomTimerGeneratorSource;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

//First start in terminal:  nc -lk 9001
public class StreamSet3Reduce {

	public static void main(String[] args) throws Exception {
		final String currentPath = System.getProperty("user.dir");
		final String inputFilename = currentPath+"/src/main/resources/inputStream3.txt";
		final String outputFilename = currentPath+"/src/main/resources/outputStream3.txt";
		Files.deleteIfExists(Paths.get(outputFilename));



		Map<String, String> config = new HashMap<>();
		config.put("host", "localhost");
		config.put("port", "9090");

		// Set up Flink configuration to expose the Web UI
		Configuration flinkConfig = new Configuration();
		flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//		flinkConfig.setString("rest.port", "8081"); // Default Flink UI port
//		flinkConfig.setString("rest.address", "localhost"); // Optional: bind to localhost

		// Use LocalStreamEnvironment to enable the Flink Web UI
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
		final ParameterTool parameterTool = ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromMap(config));
		env.getConfig().setGlobalJobParameters(parameterTool);


		DataStreamSource<String> socketStream = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port"));
		socketStream
//				.iterate()
//				.filter(line -> line.matches("[0-9].*"))
				.filter(line -> line.matches("[0-9].*"))
				.map(line -> Integer.parseInt(line))
				.setParallelism(5)
				.keyBy(line -> line % 5)
//				.sum(0)
				.reduce((acc, x) -> acc + x)
//				.maxBy("variable2")
//				.rebalance()
//				.writeToSocket("localhost", 9002, (element) -> (element+"\n").getBytes())
				.print()
				;


//		FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputFilename)).build();
//		DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source")
//				.name("file-source")
//				.uid("file-source-uid");


//		text.print();
		env.execute("Listening to socket stream");

//
////		DataSet<String> text = env.fromElements("sdf","sdf", "aaa", "avb", "aaa", "bbb", "ccc", "aba");
//
//		DataStreamSource<String> words = text.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(",")).forEach(w -> out.collect(w))).returns(TypeInformation.of(String.class));
//		DataSet<String> filtered = words.filter(line -> line.contains("a"));
//
//		DataSet<Tuple2<String,Integer>> tokenized = filtered.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
//			System.out.println("Processing: " + value);
//			for (String word : value.toLowerCase().split("\\W+")) {
//				if (!word.isEmpty()) {
//					out.collect(new Tuple2<>(word, 1));
//				}
//			}
//		}).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
//
//		DataSet<Tuple2<String,Integer>> counts = tokenized.groupBy(0).sum(1);
//
////		counts.writeAsText(outputFilename).setParallelism(1);
////		env.execute("Flink Streaming Java API Skeleton");
//
//		counts.print();
	}
}
