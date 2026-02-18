# Flink playbook

Open Flink UI on http://localhost:8081

Start Kafka dependency: `podman-compose up --build -f docker-compose-kafka.yml -d`

Create `test1` topic: `kafka-topics.sh -bootstrap-server localhost:9092 --create --topic test1` 
Produce to topic: `kafka-console-producer --bootstrap-server localhost:9092  --topic test1`

Start Flink-Iceberg dependency: `podman-compose up --build -f docker-compose-minio-nessie.yml -d`

### See the class you want to run if a different dependency setup is needed.


### Note:
- for any errors causeds by Flink java reflextion, like: 
```Unable to make field private final java.lang.Object[] java.util.Arrays$ArrayList.a accessible: module java.base does not "opens java.util" to unnamed module @27e5b378```
see `.vscode/launch.json`: Java must start with:
```
"vmArgs": "--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.nio.cs=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED --add-opens java.base/sun.util.calendar=ALL-UNNAMED"
```