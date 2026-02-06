package org.example.EventDrivenAggregates;

import java.util.Objects;

/**
 * Event fired when a job is assigned to a driver
 */
public class JobAssigned implements JobEvent {
    private String jobId;
    private long timestamp;
    private String driverId;
    private String driverName;

    public JobAssigned() {}

    public JobAssigned(String jobId, long timestamp, String driverId, String driverName) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.driverId = driverId;
        this.driverName = driverName;
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
        return "JobAssigned";
    }

    public String getDriverId() {
        return driverId;
    }

    public String getDriverName() {
        return driverName;
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

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    @Override
    public String toString() {
        return "JobAssigned{" +
                "jobId='" + jobId + '\'' +
                ", timestamp=" + timestamp +
                ", driverId='" + driverId + '\'' +
                ", driverName='" + driverName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobAssigned that = (JobAssigned) o;
        return timestamp == that.timestamp && Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp);
    }
}
