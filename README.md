# Flink playbook

Open Flink UI on http://localhost:8081

Start Kafka dependency: `podman-compose up --build -f docker-compose-kafka.yml -d`

Create `test1` topic: `kafka-topics.sh -bootstrap-server localhost:9092 --create --topic test1` 
Produce to topic: `kafka-console-producer --bootstrap-server localhost:9092  --topic test1`

Start Flink-Iceberg dependency: `podman-compose up --build -f docker-compose-minio-nessie.yml -d`
