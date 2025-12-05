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

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

//First start in terminal:  nc -lk 9001
public class StreamSet5State {

	public static void main(String[] args) throws Exception {
		final String currentPath = System.getProperty("user.dir");
		final String inputFilename = currentPath+"/src/main/resources/inputStream3.txt";
		final String outputFilename = currentPath+"/src/main/resources/outputStream3.txt";
		Files.deleteIfExists(Paths.get(outputFilename));

		Map<String, String> config = new HashMap<>();
		config.put("host", "localhost");
		config.put("port", "9001");

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
				.filter(line -> line.matches("[0-9].*"))
				.map(line -> Integer.parseInt(line))
				.setParallelism(5)
				.keyBy(line -> line % 5)
				.flatMap(new RichFlatMapFunction<Integer,Integer>() {
					private static final long serialVersionUID = 1L;

					private transient ListState<Integer> elements;
					private transient ValueState<Integer> count;

					@Override
					public void flatMap(Integer element, Collector<Integer> out) throws Exception {
						elements.add(element);

						Integer currentCount = count.value();
						currentCount += 1;
						count.update(currentCount);

						if (currentCount >= 5) {
							Integer sum = 0;
							for (Integer v : elements.get()) { sum += v; }
							out.collect(sum);
							elements.clear();
							count.clear();
						}
					}

					@Override
					public void setRuntimeContext(RuntimeContext t) {
						super.setRuntimeContext(t);
					}

					@Override
					public RuntimeContext getRuntimeContext() {
						return super.getRuntimeContext();
					}

					@Override
					public IterationRuntimeContext getIterationRuntimeContext() {
						return super.getIterationRuntimeContext();
					}

					@Override
					public void open(Configuration parameters) throws Exception {
						ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("elements", Integer.class);
						elements = getRuntimeContext().getListState(descriptor);

						ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<>("count", Integer.class, 0);
						count = getRuntimeContext().getState(descriptor2);
					}

					@Override
					public void close() throws Exception {
						super.close();
					}
				})
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
	}

	public static class CustomElementTrigger extends Trigger<String, GlobalWindow> {
		private static final long serialVersionUID = 1L;
		private static final long WINDOW_TIMEOUT = 5000L; // 5 seconds

		@Override
		public TriggerResult onElement(String element, long timestamp, GlobalWindow window, TriggerContext ctx) {
			if ("0".equals(element)) {
				return TriggerResult.FIRE_AND_PURGE;
			}
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public void clear(GlobalWindow window, TriggerContext ctx) {}
	}
}
