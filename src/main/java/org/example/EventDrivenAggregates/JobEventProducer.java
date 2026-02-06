package org.example.EventDrivenAggregates;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Producer to generate sample job events and send them to Kafka
 * Run this to generate test data for the EventDrivenAggregates job
 */
public class JobEventProducer {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();

    private static final Long EVENT_DELAY = 11000L;

    public static void main(String[] args) throws Exception {
        // Configure Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("Starting to produce job events...");

        // Generate i complete job lifecycles
        for (int i = 0; i < 1; i++) {
            String jobId = "JOB-" + UUID.randomUUID().toString().substring(0, 8);
            long baseTime = System.currentTimeMillis();

            System.out.println("\n=== Creating job lifecycle for " + jobId + " ===");

            // 1. JobCreated
            JobCreated created = new JobCreated(
                    jobId,
                    baseTime,
                    "CUST-" + (1000 + i),
                    "Pickup Address " + (i + 1),
                    "Delivery Address " + (i + 1)
            );
            sendEvent(producer, created);
            Thread.sleep(EVENT_DELAY);

            // 2. JobAssigned (5-10 seconds later)
            JobAssigned assigned = new JobAssigned(
                    jobId,
                    baseTime + 5000 + random.nextInt(5000),
                    "DRIVER-" + (100 + random.nextInt(10)),
                    "Driver " + (char)('A' + random.nextInt(26))
            );
            sendEvent(producer, assigned);
            Thread.sleep(EVENT_DELAY);

            // 3. JobFetched (10-20 seconds after assignment)
            JobFetched fetched = new JobFetched(
                    jobId,
                    assigned.getTimestamp() + 10000 + random.nextInt(10000),
                    created.getPickupLocation()
            );
            sendEvent(producer, fetched);
            Thread.sleep(EVENT_DELAY);

            // 4. JobDelivered (20-40 seconds after fetched)
            JobDelivered delivered = new JobDelivered(
                    jobId,
                    fetched.getTimestamp() + 20000 + random.nextInt(20000),
                    created.getDeliveryLocation(),
                    "SIGNATURE-" + UUID.randomUUID().toString().substring(0, 8)
            );
            sendEvent(producer, delivered);
            Thread.sleep(EVENT_DELAY);

            System.out.println("Completed job lifecycle for " + jobId);
        }

        System.out.println("\n=== All events produced successfully! ===");
        producer.close();
    }

    private static void sendEvent(KafkaProducer<String, String> producer, JobEvent event) throws Exception {
        String json = mapper.writeValueAsString(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "job-events",
                event.getJobId(),
                json
        );
        producer.send(record);
        System.out.println("Sent: " + event.getEventType() + " for " + event.getJobId() + " at " + event.getTimestamp());
    }
}
