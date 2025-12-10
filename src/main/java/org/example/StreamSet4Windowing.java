package org.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.OutputTag;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

//First start in terminal:  nc -lk 9001
public class StreamSet4Windowing {

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
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		final ParameterTool parameterTool = ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromMap(config));
		env.getConfig().setGlobalJobParameters(parameterTool);


		DataStreamSource<String> socketStream = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port"));

//		stream window
		socketStream
//				.windowAll(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
//				.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
				.windowAll(GlobalWindows.create())
//				.allowedLateness(Time.seconds(5))
				.trigger(new CustomElementTrigger()) // triggered when element "0" is received
//				.evictor(CountEvictor.of(3))
				.evictor(DeltaEvictor.of(10, (String a, String b) -> Math.abs(Integer.parseInt(a) - Integer.parseInt(b)))) // evicts elements that differ by more than 10
				.reduce((acc, x) -> acc + "+" + x)
				.print()
				;
//		KeyedProcessFunction

//      keyed window
//		socketStream
//				.filter(line -> line.matches("[0-9].*"))
//				.map(line -> Integer.parseInt(line))
//				.setParallelism(5)
//				.keyBy(line -> line % 5)
//				.window(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
////				.sum(0)
//				.reduce((acc, x) -> acc + x)
////				.maxBy("variable2")
////				.rebalance()
////				.writeToSocket("localhost", 9002, (element) -> (element+"\n").getBytes())
//				.print()
//				;


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
