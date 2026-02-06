# REvent sourced aggregates with Kafka input

## Run:
 - `docker compose -f docker-compose-kafka.yml up`
 - create Kafka topic: `docker exec -ti kafka-processor bash -c "/opt/kafka/bin/kafka-topics.sh -bootstrap-server localhost:9092 --create --topic job-events"`
 - run `EventDrivenAggregates.java` to start the Flink job that runs the datastream and builds the aggregate.
 - separately run `JobEventProducer` to generate job events.
 - 