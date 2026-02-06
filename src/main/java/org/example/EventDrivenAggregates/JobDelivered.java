package org.example.EventDrivenAggregates;

import java.util.Objects;

/**
 * Event fired when a job is successfully delivered
 */
public class JobDelivered implements JobEvent {
    private String jobId;
    private long timestamp;
    private String location;
    private String signature;

    public JobDelivered() {}

    public JobDelivered(String jobId, long timestamp, String location, String signature) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.location = location;
        this.signature = signature;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String getEventType() {
        return "JobDelivered";
    }

    public String getLocation() {
        return location;
    }

    public String getSignature() {
        return signature;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setEventType(String eventType) {
        // No-op since event type is fixed
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    @Override
    public String toString() {
        return "JobDelivered{" +
                "jobId='" + jobId + '\'' +
                ", timestamp=" + timestamp +
                ", location='" + location + '\'' +
                ", signature='" + signature + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobDelivered that = (JobDelivered) o;
        return timestamp == that.timestamp && Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp);
    }
}
