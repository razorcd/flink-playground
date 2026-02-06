# Reads from Kafka topic and sinks to Iceberg table using datastream.
## Run:
 - `docker compose -f docker-compose-iceberg-rest.yml up`
 - `docker compose -f docker-compose-kafka.yml up`
 - create Kafka topic: `docker exec -ti kafka-processor bash -c "/opt/kafka/bin/kafka-topics.sh -bootstrap-server localhost:9092 --create --topic realtime-data-topic"`
 - run `org.example.FlinkIcebergPipes.ReadIceberg.java` to create Iceberg table if missing and query all data.
 - start this `java org.example.FlinkIcebergPipes.KafkaToIcebergFlinkPipe` standalone job to read from both sources.
 - in terminal produce Kafka event: `docker exec -ti kafka-processor bash -c "/opt/kafka/bin/kafka-console-producer.sh -bootstrap-server localhost:9092 --topic realtime-data-topic"`
   - payload: `{"event_id": "event3", "user_id":"1002"}`
 - run `org.example.FlinkIcebergPipes.ReadIceberg.java` query all bounded Iceberg data.