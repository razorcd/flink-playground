package org.example.EventDrivenAggregates;

import java.util.Objects;

/**
 * Event fired when a new job is created
 */
public class JobCreated implements JobEvent {
    private String jobId;
    private long timestamp;
    private String customerId;
    private String pickupLocation;
    private String deliveryLocation;

    public JobCreated() {}

    public JobCreated(String jobId, long timestamp, String customerId, 
                      String pickupLocation, String deliveryLocation) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.customerId = customerId;
        this.pickupLocation = pickupLocation;
        this.deliveryLocation = deliveryLocation;
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
        return "JobCreated";
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getPickupLocation() {
        return pickupLocation;
    }

    public String getDeliveryLocation() {
        return deliveryLocation;
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

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public void setPickupLocation(String pickupLocation) {
        this.pickupLocation = pickupLocation;
    }

    public void setDeliveryLocation(String deliveryLocation) {
        this.deliveryLocation = deliveryLocation;
    }

    @Override
    public String toString() {
        return "JobCreated{" +
                "jobId='" + jobId + '\'' +
                ", timestamp=" + timestamp +
                ", customerId='" + customerId + '\'' +
                ", pickupLocation='" + pickupLocation + '\'' +
                ", deliveryLocation='" + deliveryLocation + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobCreated that = (JobCreated) o;
        return timestamp == that.timestamp && Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp);
    }
}
