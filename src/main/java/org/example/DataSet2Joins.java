package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;

import java.nio.file.Files;
import java.nio.file.Paths;


public class DataSet2Joins {

    public static void main(String[] args) throws Exception {
        final String currentPath = System.getProperty("user.dir");
        final String inputFilename = currentPath+"/src/main/resources/input2.txt";
        final String outputFilename = currentPath+"/src/main/resources/output2.txt";
        Files.deleteIfExists(Paths.get(outputFilename));

        // The DataSet API was removed in Flink 2.0, bounded DataStream jobs in BATCH mode replace it
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(parameterTool);

        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputFilename)).build();
        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        System.out.println("Source data from " + inputFilename);
        text.print();
        System.out.println();

        DataStream<Tuple2<Integer,String>> lines = text
                .map(line -> {
                    String[] split = line.split(",");
                    return new Tuple2<Integer, String>(Integer.parseInt(split[0]), split[1]);
                })
                .returns(new TypeHint<Tuple2<Integer, String>>() {});
        DataStream<Tuple2<Integer,String>> filteredText = lines.filter(line -> line.f1.contains("a"));
        filteredText.print();



        DataStream<Integer> numbers = env.fromData(0,1,2,3,4,5,6,7,8);
        DataStream<Tuple2<Integer,String>> numberesTuple = numbers.map(number -> {
                    String processedNumber = "number:"+number;
                    return new Tuple2<Integer, String>(number, processedNumber);
                })
                .returns(new TypeHint<Tuple2<Integer, String>>() {});
//		DataStream<String> words = text.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(",")).forEach(w -> out.collect(w))).returns(TypeInformation.of(String.class));

        DataStream<Tuple3<Integer,String,String>> joinedData = filteredText
                // DataStream has no outer join, a coGroup over the whole bounded input emulates the left outer join
                .coGroup(numberesTuple)
                .where(left -> left.f0).equalTo(right -> right.f0)
                .window(GlobalWindows.createWithEndOfStreamTrigger())
                .apply((Iterable<Tuple2<Integer, String>> lefts, Iterable<Tuple2<Integer, String>> rights, Collector<Tuple3<Integer,String,String>> out) -> {
                    for (Tuple2<Integer, String> left : lefts) {
                        boolean matched = false;
                        for (Tuple2<Integer, String> right : rights) {
                            matched = true;
                            out.collect(new Tuple3<>(left.f0, left.f1, right.f1));
                        }
                        if (!matched) {
                            out.collect(new Tuple3<>(left.f0, left.f1, null));
                        }
                    }
                }, TypeInformation.of(new TypeHint<Tuple3<Integer,String,String>>() {}));


        DataStream<Tuple3<String,String,Integer>> tokenized = joinedData
                .flatMap((Tuple3<Integer,String,String> values, Collector<Tuple3<String,String,Integer>> out) -> {
                    System.out.println("Processing: " + values);
                    out.collect(new Tuple3<String,String,Integer>(values.f1, values.f2, 1));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String,String,Integer>>() {}));
            DataStream<Tuple3<String,String,Integer>> counts = tokenized.keyBy(t -> t.f0).sum(2);

//		counts.writeAsText(outputFilename).setParallelism(1);

        counts.print();
        env.execute("DataSet2Joins");
    }
}
