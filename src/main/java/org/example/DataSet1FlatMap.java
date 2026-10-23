package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;


public class DataSet1FlatMap {

	public static void main(String[] args) throws Exception {
		final String currentPath = System.getProperty("user.dir");
		final String inputFilename = currentPath+"/src/main/resources/input.txt";
		final String outputFilename = currentPath+"/src/main/resources/output.txt";
		Files.deleteIfExists(Paths.get(outputFilename));

		// The DataSet API was removed in Flink 2.0, bounded DataStream jobs in BATCH mode replace it
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		env.getConfig().setGlobalJobParameters(parameterTool);

		FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputFilename)).build();
		DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
//		DataStream<String> text = env.fromData("sdf","sdf", "aaa", "avb", "aaa", "bbb", "ccc", "aba");

		DataStream<String> words = text.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(",")).forEach(w -> out.collect(w))).returns(TypeInformation.of(String.class));
		DataStream<String> filtered = words.filter(line -> line.contains("a"));

		DataStream<Tuple2<String,Integer>> tokenized = filtered.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
			System.out.println("Processing: " + value);
			for (String word : value.toLowerCase().split("\\W+")) {
				if (!word.isEmpty()) {
					out.collect(new Tuple2<>(word, 1));
				}
			}
		}).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

		DataStream<Tuple2<String,Integer>> counts = tokenized.keyBy(t -> t.f0).sum(1);

//		counts.writeAsText(outputFilename).setParallelism(1);

		counts.print();
		env.execute("DataSet1FlatMap");
	}
}
