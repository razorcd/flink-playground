package org.example.FlinkHybridSource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.RowData;
import org.example.other.Event2;
import org.apache.iceberg.flink.source.reader.RowDataConverter;

public class Event2RawDataConvertor implements RowDataConverter<Event2> {

    @Override
    public Event2 apply(RowData row) {
        Event2 event = new Event2();
        event.event_id = row.getString(0).toString();
        event.user_id = row.getString(1).toString();
        return event;
    }

    @Override
    public TypeInformation<Event2> getProducedType() {
        return TypeInformation.of(Event2.class);
    }
    
}
