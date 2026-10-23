package org.example;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.other.CustomTimerGeneratorSourceHeartbeat2;


public class StreamSet14CustomTimeGeneratorNumbers {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		Map<String, String> config = new HashMap<>();
		config.put("host", "localhost");
		config.put("port", "9001");
		
		final ParameterTool parameterTool = ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromMap(config));
		env.getConfig().setGlobalJobParameters(parameterTool);


        DataStreamSource<Long> dataStreamSource = env.setParallelism(5)
                .addSource(new CustomTimerGeneratorSourceHeartbeat2(), "timedNumberGenerator");

		// start: nc -lk 9001
		// TCP listener as input stream
		SingleOutputStreamOperator<Long> socketStream = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port")).map(x -> Long.valueOf(x));

		socketStream.union(dataStreamSource)
				.windowAll(TumblingProcessingTimeWindows.of(Time.of(30, TimeUnit.SECONDS)))
//				.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
				// .windowAll(GlobalWindows.create())
//				.allowedLateness(Time.seconds(5))
				// .trigger(new NumberFiresTrigger()) // numbers fire the window early, the 5s timer still fires it too
				// .trigger(new CustomElementTrigger()) // triggered when element "0" is received
//				.evictor(CountEvictor.of(3))
				.process(new IgnoreNumbersWindowFunction()) // numbers are left out of the result
				.print()
				;

		env.execute("Listening to socket stream");
	}


	public static class IgnoreNumbersWindowFunction extends ProcessAllWindowFunction<Long, Long, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void process(Context context, Iterable<Long> elements, Collector<Long> out) {
			Long result = 0L;
			for (Long element : elements) {
				if (element == -1) {
					continue;
				}
				result = result + element;
			}
			out.collect(result);
		}
	}

}
