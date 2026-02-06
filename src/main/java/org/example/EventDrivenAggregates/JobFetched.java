package org.example.EventDrivenAggregates;

import java.util.Objects;

/**
 * Event fired when a driver fetches/picks up the package
 */
public class JobFetched implements JobEvent {
    private String jobId;
    private long timestamp;
    private String location;

    public JobFetched() {}

    public JobFetched(String jobId, long timestamp, String location) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.location = location;
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
        return "JobFetched";
    }

    public String getLocation() {
        return location;
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

    @Override
    public String toString() {
        return "JobFetched{" +
                "jobId='" + jobId + '\'' +
                ", timestamp=" + timestamp +
                ", location='" + location + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobFetched that = (JobFetched) o;
        return timestamp == that.timestamp && Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp);
    }
}
