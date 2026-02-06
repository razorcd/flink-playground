package org.example.FlinkIcebergPipes;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.example.other.Event2;

public class Event2ToRowDataMapper implements MapFunction<Event2, RowData> {

    @Override
    public RowData map(Event2 event) throws Exception {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, StringData.fromString(event.event_id));
        row.setField(1, StringData.fromString(event.user_id));
        return row;
    }
}
