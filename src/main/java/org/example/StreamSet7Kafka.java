package org.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

//First start in terminal:  nc -lk 9001
public class StreamSet7Kafka {

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

		// Use LocalStreamEnvironment to enable the Flink Web UI
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
		final ParameterTool parameterTool = ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromMap(config));
		env.getConfig().setGlobalJobParameters(parameterTool);


		// KafkaSource (new API)
		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("test1")
				.setGroupId("flink-group-1")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		// Add Kafka source using new API
		DataStreamSource<String> kafkaStream = env.fromSource(
				kafkaSource,
				WatermarkStrategy.noWatermarks(),
				"Kafka Source"
		);

		kafkaStream
				.filter(line -> line.matches("[0-9].*"))
				.map(Integer::parseInt)
				.setParallelism(5)
				.keyBy(line -> line % 5)
				.reduce((acc, x) -> acc + x)
				.print();


		env.execute("Listening to socket stream");

	}
}
