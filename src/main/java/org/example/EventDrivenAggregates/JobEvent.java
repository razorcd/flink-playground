package org.example.EventDrivenAggregates;

import java.io.Serializable;

/**
 * Base interface for all job events
 */
public interface JobEvent extends Serializable {
    String getJobId();
    long getTimestamp();
    String getEventType();
}
