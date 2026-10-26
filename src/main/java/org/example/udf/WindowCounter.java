package org.example.udf;

import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_ON_TIME;
import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

// PTF counting v per 30s tumbling window and partition, empty windows included (total 0), see FlinkSQL15HeartbeatWindowsPTF.
// Input: a table with p BIGINT, v BIGINT and an event time column passed as on_time. Needs one row per partition to start.
// Packaged on its own by `mvn package` as target/FlinkExamples-1.0-SNAPSHOT-udf.jar:
//   CREATE FUNCTION WindowCount AS 'org.example.udf.WindowCount' USING JAR '/path/to/FlinkExamples-1.0-SNAPSHOT-udf.jar';
@DataTypeHint("ROW<p BIGINT, window_start TIMESTAMP_LTZ(3), window_end TIMESTAMP_LTZ(3), total BIGINT>")
public class WindowCounter extends ProcessTableFunction<Row> {
	private static final long WINDOW_SIZE_MS = Duration.ofSeconds(30).toMillis();

	// running count per open window, keyed by window end (epoch millis)
	public static class WindowState {
		// the partition this state belongs to, the PTF only outputs the PARTITION BY column by itself
		public Long partition;
		public Map<Long, Long> count = new HashMap<>();
	}

	public void eval(Context ctx, @StateHint WindowState state, @ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME}) Row input) {
		TimeContext<Long> time = ctx.timeContext(Long.class);
		long windowEnd = time.time() - time.time() % WINDOW_SIZE_MS + WINDOW_SIZE_MS;

		state.partition = input.getFieldAs("p");

		// the start row has no value, it only opens the window
		Long value = input.getFieldAs("v");
		state.count.merge(windowEnd, value == null ? 0L : value, (acc, v) -> acc = acc + 1);

		registerWindowTimer(time, windowEnd);
	}

	public void onTimer(OnTimerContext ctx, WindowState state) {
		long windowEnd = Long.parseLong(ctx.currentTimer());
		Long total = state.count.remove(windowEnd);
		collect(Row.of(state.partition, Instant.ofEpochMilli(windowEnd - WINDOW_SIZE_MS), Instant.ofEpochMilli(windowEnd), total == null ? 0L : total));

		// the next window fires even if no row ever lands in it.
		// Once the socket closes the watermark jumps to the end of time, stop there instead of firing every future window
		TimeContext<Long> time = ctx.timeContext(Long.class);
		if (time.currentWatermark() != Long.MAX_VALUE) {
			registerWindowTimer(time, windowEnd + WINDOW_SIZE_MS);
		}
	}

	// fires once the watermark passes the last millisecond of the window, one timer per window end
	private static void registerWindowTimer(TimeContext<Long> time, long windowEnd) {
		time.registerOnTime(String.valueOf(windowEnd), windowEnd - 1);
	}
}
