package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.other.Buffer;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

//First start in terminal:  nc -lk 9090
public class StreamSet12BufferEmitLast {

	public static void main(String[] args) throws Exception {
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

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli())
//                .withWatermarkAlignment("1", Duration.ofSeconds(5), Duration.ofSeconds(1));
//                .withIdleness(Duration.ofSeconds(5))
                ;

//        WatermarkStrategy<String> watermarkStrategy2 = WatermarkStrategy
//                .forMonotonousTimestamps()
//                .
//                ;

//        env.getConfig().setAutoWatermarkInterval(5000L)
//        ;

        DataStreamSource<String> socketStream = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port"));
//        env.getConfig().registerKryoType(Tuple3.class);

//        SingleOutputStreamOperator<Tuple3<Long, Long, String>> stream =  socketStream
        SingleOutputStreamOperator<String> stream =  socketStream
                .filter(line -> line.matches("[0-9].*"))
//                .setParallelism(2)
//                .setBufferTimeout(2000)

                .assignTimestampsAndWatermarks(watermarkStrategy)
//                .forward()
//                .setParallelism(3)
                .keyBy(line -> Long.parseLong(line) % 3)
//                .map(s -> Long.parseLong(s))
                .process(new Buffer())
				;
        stream.print();



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
