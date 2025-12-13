package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;
import java.time.ZoneId;

// generates 1 minute time triggers with any parallelism.
// Always triggers at minute defined by minuteOdTheHourTrigger. minuteOdTheHourTrigger = 15 will trigger at 0, 15, 30, 45 minutes of each hour.
//
// Output:
//    2> 2025-12-21T20:10:43.436934Z
//    1> 2025-12-21T20:10:43.436934Z
//
//    2> 2025-12-21T20:11:43.420115Z
//    1> 2025-12-21T20:11:43.420115Z
//
//    1> 2025-12-21T20:12:43.418101Z
//    2> 2025-12-21T20:12:43.418317Z
//
public class StreamSet13DataGeneratorSource {

	public static void main(String[] args) throws Exception {
        Integer parallelism = 2;
        Integer rateDurationSeconds = 60; // 60 will generate 1 event per minute per parallel subtask
        Integer minuteOfTheHourTrigger = 1;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataGeneratorSource<Instant> dataGeneratorSource = new DataGeneratorSource(
                index -> Instant.now(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1d/rateDurationSeconds*parallelism), // 1 event per 10 seconds
                Types.INSTANT
                );

        DataStream<Instant> stream = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "test1")
                .forward();

        stream.filter(instant -> instant.atZone(ZoneId.systemDefault()).getMinute() % minuteOfTheHourTrigger == 0)
                .print();

		env.execute("Listening to socket stream");
	}
}
