package org.example.EventDrivenAggregates;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate representing the complete state of a job
 * Built by applying events in order
 */
public class JobAggregate implements Serializable {
    private String jobId;
    private String customerId;
    private String pickupLocation;
    private String deliveryLocation;
    private String status; // CREATED, ASSIGNED, FETCHED, DELIVERED
    private String driverId;
    private String driverName;
    private String fetchLocation;
    private String deliveryActualLocation;
    private String signature;
    private long createdAt;
    private long assignedAt;
    private long fetchedAt;
    private long deliveredAt;
    private List<String> eventHistory;

    public JobAggregate() {
        this.eventHistory = new ArrayList<>();
        this.status = "UNKNOWN";
    }

    /**
     * Apply an event to the aggregate, updating its state
     */
    public void apply(JobEvent event) {
        eventHistory.add(event.getEventType() + "@" + event.getTimestamp());
        
        if (event instanceof JobCreated) {
            applyJobCreated((JobCreated) event);
        } else if (event instanceof JobAssigned) {
            applyJobAssigned((JobAssigned) event);
        } else if (event instanceof JobFetched) {
            applyJobFetched((JobFetched) event);
        } else if (event instanceof JobDelivered) {
            applyJobDelivered((JobDelivered) event);
        }
    }

    private void applyJobCreated(JobCreated event) {
        this.jobId = event.getJobId();
        this.customerId = event.getCustomerId();
        this.pickupLocation = event.getPickupLocation();
        this.deliveryLocation = event.getDeliveryLocation();
        this.status = "CREATED";
        this.createdAt = event.getTimestamp();
    }

    private void applyJobAssigned(JobAssigned event) {
        this.driverId = event.getDriverId();
        this.driverName = event.getDriverName();
        this.status = "ASSIGNED";
        this.assignedAt = event.getTimestamp();
    }

    private void applyJobFetched(JobFetched event) {
        this.fetchLocation = event.getLocation();
        this.status = "FETCHED";
        this.fetchedAt = event.getTimestamp();
    }

    private void applyJobDelivered(JobDelivered event) {
        this.deliveryActualLocation = event.getLocation();
        this.signature = event.getSignature();
        this.status = "DELIVERED";
        this.deliveredAt = event.getTimestamp();
    }

    // Getters
    public String getJobId() {
        return jobId;
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

    public String getStatus() {
        return status;
    }

    public String getDriverId() {
        return driverId;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getFetchLocation() {
        return fetchLocation;
    }

    public String getDeliveryActualLocation() {
        return deliveryActualLocation;
    }

    public String getSignature() {
        return signature;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getAssignedAt() {
        return assignedAt;
    }

    public long getFetchedAt() {
        return fetchedAt;
    }

    public long getDeliveredAt() {
        return deliveredAt;
    }

    public List<String> getEventHistory() {
        return eventHistory;
    }

    /**
     * Calculate total time from creation to delivery in milliseconds
     */
    public long getTotalDuration() {
        if (deliveredAt > 0 && createdAt > 0) {
            return deliveredAt - createdAt;
        }
        return 0;
    }

    /**
     * Check if job is complete (delivered)
     */
    public boolean isComplete() {
        return "DELIVERED".equals(status);
    }

    @Override
    public String toString() {
        return "JobAggregate{" +
                "jobId='" + jobId + '\'' +
                ", status='" + status + '\'' +
                ", customerId='" + customerId + '\'' +
                ", driverId='" + driverId + '\'' +
                ", driverName='" + driverName + '\'' +
                ", pickupLocation='" + pickupLocation + '\'' +
                ", deliveryLocation='" + deliveryLocation + '\'' +
                ", totalDuration=" + getTotalDuration() + "ms" +
                ", eventHistory=" + eventHistory +
                '}';
    }
}
