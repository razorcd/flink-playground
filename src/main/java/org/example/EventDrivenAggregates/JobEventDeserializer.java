package org.example.EventDrivenAggregates;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deserializer for JobEvent instances from JSON
 */
public class JobEventDeserializer implements DeserializationSchema<JobEvent> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JobEvent deserialize(byte[] message) throws IOException {
        JsonNode node = objectMapper.readTree(message);
        String eventType = node.get("eventType").asText();

        switch (eventType) {
            case "JobCreated":
                return objectMapper.treeToValue(node, JobCreated.class);
            case "JobAssigned":
                return objectMapper.treeToValue(node, JobAssigned.class);
            case "JobFetched":
                return objectMapper.treeToValue(node, JobFetched.class);
            case "JobDelivered":
                return objectMapper.treeToValue(node, JobDelivered.class);
            default:
                throw new IllegalArgumentException("Unknown event type: " + eventType);
        }
    }

    @Override
    public boolean isEndOfStream(JobEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JobEvent> getProducedType() {
        return TypeInformation.of(JobEvent.class);
    }
}
