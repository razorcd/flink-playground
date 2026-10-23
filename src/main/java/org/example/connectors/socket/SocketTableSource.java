package org.example.connectors.socket;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class SocketTableSource implements ScanTableSource, SupportsSourceWatermark {

	private final String hostname;
	private final int port;
	private final DataType physicalRowType;
	// set when the table declares WATERMARK FOR ts AS SOURCE_WATERMARK()
	private boolean sourceWatermark = false;

	public SocketTableSource(String hostname, int port, DataType physicalRowType) {
		this.hostname = hostname;
		this.port = port;
		this.physicalRowType = physicalRowType;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public void applySourceWatermark() {
		sourceWatermark = true;
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
		final String hostname = this.hostname;
		final int port = this.port;
		final boolean sourceWatermark = this.sourceWatermark;
		final InternalTypeInfo<RowData> rowTypeInfo = InternalTypeInfo.of((RowType) physicalRowType.getLogicalType());

		return new DataStreamScanProvider() {
			@Override
			public DataStream<RowData> produceDataStream(ProviderContext providerContext, StreamExecutionEnvironment env) {
				DataStream<RowData> rows = env.socketTextStream(hostname, port)
						.map(line -> (RowData) GenericRowData.of(Long.valueOf(line.trim()), TimestampData.fromEpochMillis(System.currentTimeMillis())))
						.returns(rowTypeInfo);
				if (!sourceWatermark) {
					return rows;
				}
				// event time = arrival time (the ts column), the watermark keeps moving with the wall clock while the socket is quiet
				return rows.assignTimestampsAndWatermarks(WatermarkStrategy.<RowData>forGenerator(ctx -> new WallClockWatermarks())
						.withTimestampAssigner((row, previousTimestamp) -> row.getTimestamp(1, 3).getMillisecond()));
			}

			@Override
			public boolean isBounded() {
				return false;
			}
		};
	}

	@Override
	public DynamicTableSource copy() {
		SocketTableSource copy = new SocketTableSource(hostname, port, physicalRowType);
		copy.sourceWatermark = sourceWatermark;
		return copy;
	}

	@Override
	public String asSummaryString() {
		return "Socket(" + hostname + ":" + port + ")";
	}


	// emits the current wall clock as watermark on every periodic tick (pipeline.auto-watermark-interval, 200ms by default),
	// whether lines arrived or not
	public static class WallClockWatermarks implements WatermarkGenerator<RowData> {
		@Override
		public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {}

		@Override
		public void onPeriodicEmit(WatermarkOutput output) {
			output.emitWatermark(new Watermark(System.currentTimeMillis() - 1));
		}
	}
}
