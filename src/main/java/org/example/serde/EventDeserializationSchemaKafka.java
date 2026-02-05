package org.example.serde;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.other.Event2;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class EventDeserializationSchemaKafka implements DeserializationSchema<Event2> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Event2 deserialize(byte[] message) throws IOException {
        JsonNode json = objectMapper.readTree(message);
        
        Event2 event = new Event2();
        event.event_id = json.has("event_id") ? json.get("event_id").asText() : null;
        event.user_id = json.has("user_id") ? json.get("user_id").asText() : null;
        return event;
    }

    @Override
    public boolean isEndOfStream(Event2 nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event2> getProducedType() {
        return TypeInformation.of(Event2.class);
    }
}