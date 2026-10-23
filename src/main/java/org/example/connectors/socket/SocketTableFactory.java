package org.example.connectors.socket;

import java.util.List;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

// SQL connector 'socket': reads numbers, one per line, from a TCP server (e.g. nc -lk 9001).
// The table needs exactly two physical columns: the BIGINT parsed from the line and a TIMESTAMP_LTZ(3) with the arrival time.
//
//   CREATE TABLE numbers (
//     v BIGINT,
//     ts TIMESTAMP_LTZ(3),
//     WATERMARK FOR ts AS SOURCE_WATERMARK()   -- optional, watermark follows the wall clock even when no lines arrive
//   ) WITH ('connector' = 'socket', 'hostname' = 'localhost', 'port' = '9001')
public class SocketTableFactory implements DynamicTableSourceFactory {

	public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname").stringType().noDefaultValue();
	public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().noDefaultValue();

	@Override
	public String factoryIdentifier() {
		return "socket";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Set.of(HOSTNAME, PORT);
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return Set.of();
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();
		ReadableConfig options = helper.getOptions();

		DataType physicalRowType = context.getPhysicalRowDataType();
		List<LogicalType> columns = physicalRowType.getLogicalType().getChildren();
		if (columns.size() != 2
				|| columns.get(0).getTypeRoot() != LogicalTypeRoot.BIGINT
				|| columns.get(1).getTypeRoot() != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
			throw new ValidationException("The 'socket' connector needs exactly the physical columns (BIGINT, TIMESTAMP_LTZ(3)), got " + physicalRowType);
		}

		return new SocketTableSource(options.get(HOSTNAME), options.get(PORT), physicalRowType);
	}
}
