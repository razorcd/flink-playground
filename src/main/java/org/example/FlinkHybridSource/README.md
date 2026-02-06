# Reads from Kafka topic and Iceberg table, then combines them into a single stream. Useful for "historical + real-time" use cases.

## Run:
 - `docker compose -f docker-compose-iceberg-rest.yml up`
 - `docker compose -f docker-compose-kafka.yml up`
 - create Kafka topic: `docker exec -ti kafka-processor bash -c "/opt/kafka/bin/kafka-topics.sh -bootstrap-server localhost:9092 --create --topic realtime-data-topic"`
 - run org.example.FlinkHybridSource.InsertIcebergRow.java to create table and insert a row into Iceberg table.
 - start this `org.example.FlinkHybridSource.KafkaIcebergFlinkHybridSource.java` standalone job to read from both sources.
 - in terminal produce Kafka event: `docker exec -ti kafka-processor bash -c "/opt/kafka/bin/kafka-console-producer.sh -bootstrap-server localhost:9092 --topic realtime-data-topic"`
   - payload: `{"event_id": "event3", "user_id":"1002"}`
 - run `org.example.FlinkHybridSource.InsertIcebergRow.java` again, it will not add the events to the stream in realtime as Iceberg is only for historical data

 - Note: the Kafka to Iceberg ingestion is not part of this example. See `FlinkIcebergPipes`