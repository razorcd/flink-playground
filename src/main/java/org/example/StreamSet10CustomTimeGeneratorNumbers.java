package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.other.CustomTimerGeneratorSource;

public class StreamSet10CustomTimeGeneratorNumbers {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();



        env.setParallelism(5)
                .addSource(new CustomTimerGeneratorSource(), "timedNumberGenerator")
                .print();

		env.execute("Listening to socket stream");

	}
}
