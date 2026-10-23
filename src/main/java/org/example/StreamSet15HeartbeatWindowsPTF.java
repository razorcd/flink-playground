package org.example;

import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_ON_TIME;
import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.ParameterTool;

// Same job as StreamSet14CustomTimeGeneratorNumbers: sum the socket numbers in 30s tumbling windows, and still emit
// windows without any socket input. The whole pipeline is Flink SQL, the window is a Process Table Function (PTF),
// and the PTF itself fires the empty windows, no heartbeat source needed:
// - PTF timers only support event time, so the 'socket' connector (org.example.connectors.socket) emits a watermark
//   that follows the wall clock even when no rows arrive, exposed as SOURCE_WATERMARK()
// - every time a window closes, the PTF registers the timer of the next one
// - a PTF only runs when a row arrives, so one start row per partition (v = NULL) opens its first window at startup
// The PTF runs over 3 parallel instances, one per partition, each partition has its own windows, state and timers.
// start: nc -lk 9001, then send lines "<partition>,<number>" with partition 0, 1 or 2, e.g. "1,5"
public class StreamSet15HeartbeatWindowsPTF {

	private static final int PARTITIONS = 3;

	public static void main(String[] args) throws Exception {
		Map<String, String> config = new HashMap<>();
		config.put("host", "localhost");
		config.put("port", "9001");

		final ParameterTool parameterTool = ParameterTool.fromMap(config).mergeWith(ParameterTool.fromArgs(args)); // args override the defaults

		final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

		// 3 parallel instances for the PTF and the sink (SET 'parallelism.default' = '3' in the SQL client).
		// The socket source itself always runs as a single instance, a TCP connection can't be split
		tEnv.getConfig().set("parallelism.default", String.valueOf(PARTITIONS));

		// TCP listener as input stream, line "1,5" gives p = 1, v = 5, ts = arrival time
		tEnv.executeSql(
				"CREATE TEMPORARY TABLE socket_numbers (\n" +
				"  p BIGINT,\n" +
				"  v BIGINT,\n" +
				"  ts TIMESTAMP_LTZ(3),\n" +
				"  WATERMARK FOR ts AS SOURCE_WATERMARK()\n" +
				") WITH (\n" +
				"  'connector' = 'socket',\n" +
				"  'hostname' = '" + parameterTool.get("host") + "',\n" +
				"  'port' = '" + parameterTool.get("port") + "'\n" +
				")"
		);

		// one row per partition (p = 0, 1, 2) at startup so each PTF partition can register its first window timer,
		// the source finishes right after
		tEnv.executeSql(
				"CREATE TEMPORARY TABLE start_rows (\n" +
				"  p BIGINT,\n" +
				"  v BIGINT,\n" +
				"  ts AS CURRENT_TIMESTAMP,\n" +
				"  WATERMARK FOR ts AS ts\n" +
				") WITH (\n" +
				"  'connector' = 'datagen',\n" +
				"  'fields.p.kind' = 'sequence',\n" +
				"  'fields.p.start' = '0',\n" +
				"  'fields.p.end' = '" + (PARTITIONS - 1) + "',\n" +
				"  'fields.v.null-rate' = '1.0'\n" +
				")"
		);

		// Flink spreads partition keys over the parallel instances by hash, and p = 0, 1, 2 all hash to the same instance.
		// route is the key the PTF partitions by, picked so that each partition lands on its own instance.
		// The values only hold for parallelism 3 and the default max parallelism 128, other keys keep their own value
		tEnv.executeSql(
				"CREATE TEMPORARY VIEW events AS\n" +
				"SELECT p, v, ts,\n" +
				"  CASE p WHEN 0 THEN 3 WHEN 1 THEN 4 WHEN 2 THEN 8 ELSE p END AS route\n" +
				"FROM (\n" +
				"  SELECT p, v, ts FROM socket_numbers\n" +
				"  UNION ALL\n" +
				"  SELECT p, v, ts FROM start_rows\n" +
				")"
		);

		// every parallel sink instance prints with its own prefix, e.g. "2> +I[1, ...]"
		tEnv.executeSql(
				"CREATE TEMPORARY TABLE window_sums (\n" +
				"  p BIGINT,\n" +
				"  window_start TIMESTAMP_LTZ(3),\n" +
				"  window_end TIMESTAMP_LTZ(3),\n" +
				"  total BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'print'\n" +
				")"
		);

		tEnv.executeSql("CREATE TEMPORARY SYSTEM FUNCTION TumblingSum AS '" + TumblingSum.class.getName() + "'");

		// PARTITION BY route: one parallel PTF instance per partition, the PTF output starts with the route column
		tEnv.executeSql(
				"INSERT INTO window_sums\n" +
				"SELECT p, window_start, window_end, total\n" +
				"FROM TumblingSum(\n" +
				"  input => TABLE events PARTITION BY route,\n" +
				"  on_time => DESCRIPTOR(ts),\n" +
				"  uid => 'tumbling-sum'\n" +
				")"
		).await();
	}


	@DataTypeHint("ROW<p BIGINT, window_start TIMESTAMP_LTZ(3), window_end TIMESTAMP_LTZ(3), total BIGINT>")
	public static class TumblingSum extends ProcessTableFunction<Row> {
		private static final long WINDOW_SIZE_MS = Duration.ofSeconds(30).toMillis();

		// running sum per open window, keyed by window end (epoch millis)
		public static class WindowState {
			// the partition this state belongs to, the PTF only outputs the PARTITION BY column (route) by itself
			public Long partition;
			public Map<Long, Long> sums = new HashMap<>();
		}

		public void eval(Context ctx, @StateHint WindowState state, @ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME}) Row input) {
			TimeContext<Long> time = ctx.timeContext(Long.class);
			long windowEnd = time.time() - time.time() % WINDOW_SIZE_MS + WINDOW_SIZE_MS;

			state.partition = input.getFieldAs("p");

			// the start row has no value, it only opens the window
			Long value = input.getFieldAs("v");
			state.sums.merge(windowEnd, value == null ? 0L : value, Long::sum);

			registerWindowTimer(time, windowEnd);
		}

		public void onTimer(OnTimerContext ctx, WindowState state) {
			long windowEnd = Long.parseLong(ctx.currentTimer());
			Long total = state.sums.remove(windowEnd);
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

}
