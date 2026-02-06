package org.example.EventDrivenAggregates;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Flink job that builds event-driven aggregates from Kafka job events
 * Demonstrates how to create JobAggregate from individual events
 */
public class EventDrivenAggregates {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Configure Kafka source
        KafkaSource<JobEvent> source = KafkaSource.<JobEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("job-events")
                .setGroupId("job-aggregate-consumer-1")
                // .setStartingOffsets(OffsetsInitializer.earliest())
                // .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new JobEventDeserializer())
                .build();

        // // Create watermark strategy for event time processing
        // WatermarkStrategy<JobEvent> watermarkStrategy = WatermarkStrategy
        //         .<JobEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        //         .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        // Read events from Kafka
        DataStream<JobEvent> events = env
                .fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Job Events Source");

        // Print incoming events
        // events.print("Event");


        // // Build aggregates by jobId using keyed state
        // DataStream<JobAggregate> aggregates = events
        //         .keyBy(JobEvent::getJobId)
        //         .window(TumblingEventTimeWindows.of(Time.seconds(30)))
        //         .aggregate(new JobAggregateFunction());

        // Build aggregates by jobId using keyed state (no windowing)
        DataStream<JobAggregate> aggregates = events
                .keyBy(JobEvent::getJobId)
                .process(new JobAggregateProcessFunction());

        // Print completed aggregates
        aggregates.print("Aggregate");

        // Filter and print only completed jobs
        aggregates
                .filter(JobAggregate::isComplete)
                .map(agg -> String.format(
                        "COMPLETED JOB: %s - Customer: %s, Driver: %s, Duration: %dms",
                        agg.getJobId(),
                        agg.getCustomerId(),
                        agg.getDriverName(),
                        agg.getTotalDuration()
                ))
                .print("Completed");

        // Execute the job
        env.execute("Event Driven Aggregates Job");
    }

    // /**
    //  * Aggregate function that builds JobAggregate from events
    //  */Process function that builds JobAggregate from events using keyed state
    //  * Each jobId maintains its own aggregate state that gets updated as events arrive
    //  */
    public static class JobAggregateProcessFunction extends KeyedProcessFunction<String, JobEvent, JobAggregate> {
        
        private transient ValueState<JobAggregate> aggregateState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<JobAggregate> descriptor = 
                new ValueStateDescriptor<>("job-aggregate", JobAggregate.class);
            aggregateState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(JobEvent event, Context ctx, Collector<JobAggregate> out) throws Exception {
            // Get current aggregate or create new one
            JobAggregate aggregate = aggregateState.value();
            if (aggregate == null) {
                aggregate = new JobAggregate();
            }

            // Apply the event to the aggregate
            aggregate.apply(event);

            // Update state
            aggregateState.update(aggregate);

            // Emit the updated aggregate
            out.collect(aggregate);
        }
    }
}
