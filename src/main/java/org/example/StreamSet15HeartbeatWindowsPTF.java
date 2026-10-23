package org.example;

import static org.apache.flink.table.annotation.ArgumentTrait.OPTIONAL_PARTITION_BY;
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
// - a PTF only runs when a row arrives, so a single start row (v = NULL) opens the first window at startup
// start: nc -lk 9001
public class StreamSet15HeartbeatWindowsPTF {

	public static void main(String[] args) throws Exception {
		Map<String, String> config = new HashMap<>();
		config.put("host", "localhost");
		config.put("port", "9001");

		final ParameterTool parameterTool = ParameterTool.fromMap(config).mergeWith(ParameterTool.fromArgs(args)); // args override the defaults

		final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

		// a single parallel instance, like windowAll (SET 'parallelism.default' = '1' in the SQL client)
		tEnv.getConfig().set("parallelism.default", "1");

		// TCP listener as input stream, ts = arrival time
		tEnv.executeSql(
				"CREATE TEMPORARY TABLE socket_numbers (\n" +
				"  v BIGINT,\n" +
				"  ts TIMESTAMP_LTZ(3),\n" +
				"  WATERMARK FOR ts AS SOURCE_WATERMARK()\n" +
				") WITH (\n" +
				"  'connector' = 'socket',\n" +
				"  'hostname' = '" + parameterTool.get("host") + "',\n" +
				"  'port' = '" + parameterTool.get("port") + "'\n" +
				")"
		);

		// one row at startup so the PTF can register its first window timer, the source finishes right after
		tEnv.executeSql(
				"CREATE TEMPORARY TABLE start_row (\n" +
				"  v BIGINT,\n" +
				"  ts AS CURRENT_TIMESTAMP,\n" +
				"  WATERMARK FOR ts AS ts\n" +
				") WITH (\n" +
				"  'connector' = 'datagen',\n" +
				"  'number-of-rows' = '1',\n" +
				"  'fields.v.null-rate' = '1.0'\n" +
				")"
		);

		tEnv.executeSql(
				"CREATE TEMPORARY VIEW events AS\n" +
				"SELECT v, ts FROM socket_numbers\n" +
				"UNION ALL\n" +
				"SELECT v, ts FROM start_row"
		);

		tEnv.executeSql("CREATE TEMPORARY SYSTEM FUNCTION TumblingSum AS '" + TumblingSum.class.getName() + "'");

		// no PARTITION BY: the whole table goes to one PTF instance, like windowAll
		tEnv.executeSql(
				"SELECT window_start, window_end, total\n" +
				"FROM TumblingSum(\n" +
				"  input => TABLE events,\n" +
				"  on_time => DESCRIPTOR(ts),\n" +
				"  uid => 'tumbling-sum'\n" +
				")"
		).print();
	}


	@DataTypeHint("ROW<window_start TIMESTAMP_LTZ(3), window_end TIMESTAMP_LTZ(3), total BIGINT>")
	public static class TumblingSum extends ProcessTableFunction<Row> {
		private static final long WINDOW_SIZE_MS = Duration.ofSeconds(30).toMillis();

		// running sum per open window, keyed by window end (epoch millis)
		public static class WindowState {
			public Map<Long, Long> sums = new HashMap<>();
		}

		public void eval(Context ctx, @StateHint WindowState state, @ArgumentHint({SET_SEMANTIC_TABLE, OPTIONAL_PARTITION_BY, REQUIRE_ON_TIME}) Row input) {
			TimeContext<Long> time = ctx.timeContext(Long.class);
			long windowEnd = time.time() - time.time() % WINDOW_SIZE_MS + WINDOW_SIZE_MS;

			// the start row has no value, it only opens the window
			Long value = input.getFieldAs("v");
			state.sums.merge(windowEnd, value == null ? 0L : value, Long::sum);

			registerWindowTimer(time, windowEnd);
		}

		public void onTimer(OnTimerContext ctx, WindowState state) {
			long windowEnd = Long.parseLong(ctx.currentTimer());
			Long total = state.sums.remove(windowEnd);
			collect(Row.of(Instant.ofEpochMilli(windowEnd - WINDOW_SIZE_MS), Instant.ofEpochMilli(windowEnd), total == null ? 0L : total));

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
