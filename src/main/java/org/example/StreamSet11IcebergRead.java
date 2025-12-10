package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.IcebergTableSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.reader.RowDataConverter;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

//First start in terminal:  nc -lk 9001
public class StreamSet11IcebergRead {

        public static void main(String[] args) throws Exception {

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
            env.enableCheckpointing(10000);
            env.setParallelism(1);

            // --- 1. Configure Hadoop for MinIO (S3-Compatible) ---
            // These settings tell Iceberg where its data files (parquet/orc) are located.
            Configuration hadoopConf = new Configuration();
            // S3 Configuration for MinIO (Keep these settings from the previous answer)
            hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//            hadoopConf.set("fs.s3a.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
            hadoopConf.set("fs.s3a.endpoint", "http://localhost:9002"); // Your MinIO endpoint
//            hadoopConf.set("fs.s3a.endpoint.region", "us-east-1");
            hadoopConf.set("fs.s3a.access.key", "admin");
            hadoopConf.set("fs.s3a.secret.key", "password");
//            hadoopConf.set("fs.s3a.access-key-id", "password");
            hadoopConf.set("fs.s3a.path.style.access", "true");
//            hadoopConf.set("fs.s3a.path.style", "true");
//            hadoopConf.set("fs.s3a.ssl.enabled", "false");


//            CREATE CATALOG iceberg WITH (
//            'type' = 'iceberg',
//            'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',  -- Use REST catalog
//            'uri' = 'http://iceberg-rest:8181',                     -- REST catalog server URL
//            'warehouse' = 's3://warehouse/',                        -- Warehouse location
//            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',       -- S3 file IO
//            's3.endpoint' = 'http://minio:9000',                    -- MinIO endpoint
//            's3.path-style-access' = 'true',                        -- Enable path-style access
//            'client.region' = 'us-east-1',                          -- S3 region
//            's3.access-key-id' = 'admin',                           -- MinIO access key
//            's3.secret-access-key' = 'password'                     -- MinIO secret key
//);



            // --- 2. Define Nessie Catalog Properties ---

//            final String catalogName = "nessie_catalog";
            final String warehousePath = "s3a://lakehouse/examples";

            // Replace with your local Nessie server endpoint (default is 33000)


            Map<String, String> catalogProperties = new HashMap<>();
            // Set Iceberg Catalog Type to Nessie
//            catalogProperties.put("catalog-impl", "org.projectnessie.iceberg.nessie.NessieCatalog");
            catalogProperties.put("catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
            catalogProperties.put("type", "iceberg");
            // Nessie Specific Properties
            catalogProperties.put("uri", "http://localhost:8181");
//            catalogProperties.put("'client.region", "'us-east-1");
//            catalogProperties.put("ref", "main");
//            catalogProperties.put("authentication.type", "NONE"); // Or BASIC if secured

            // Warehouse property tells Iceberg where to find the data files (MinIO S3 bucket)
//            catalogProperties.put("warehouse", warehousePath);
            catalogProperties.put("warehouse", "s3a://warehouse/");
            catalogProperties.put("format", "parquet");

            // S3 properties for the REST Catalog's FileIO
            catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            catalogProperties.put("s3.endpoint", "http://localhost:9002");
            catalogProperties.put("s3.path-style-access", "true");
            catalogProperties.put("s3.access-key-id", "admin");
            catalogProperties.put("s3.secret-access-key", "password");
//

            // --- 3. Create the Table Loader ---

//            TableLoader tableLoader = TableLoader.fromCatalog(CatalogLoader.hadoop("HadoopCatalog", hadoopConf, catalogProperties), TableIdentifier.of("names"));
            TableLoader tableLoader = TableLoader.fromCatalog(CatalogLoader.rest("RestCatalog", hadoopConf, catalogProperties), TableIdentifier.of("db.rawdatastream_sink"));
//            TableLoader tableLoader = TableLoader.fromHadoopTable("s3a://lakehouse/examples/names", hadoopConf);
//            format' = 'parquet

//
//            DataStream<RowData> icebergStream = FlinkSource
//                    .forRowData()
//                    .env(env)
//                    .tableLoader(tableLoader)
//                    .streaming(true)
////                    .startSnapshotId(3821550127947081111L)
//                    .monitorInterval(Duration.ofSeconds(5)) // Checks Nessie for new commits every 10 seconds
//                    .project(org.apache.flink.table.api.TableSchema.builder()
//                            .field("event_id", DataTypes.STRING())
//                            .build())
//                    .build();

            IcebergSource<RowData> source = IcebergSource
                    .forRowData()
                    .tableLoader(tableLoader)
                    .assignerFactory(new SimpleSplitAssignerFactory())
                    .streaming(true)
                    .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
                    .startSnapshotId(3821550127947080000L)
                    .monitorInterval(Duration.ofSeconds(5)) // Checks Nessie for new commits every 10 seconds
                    .project(org.apache.flink.table.api.TableSchema.builder()
                            .field("event_id", DataTypes.STRING())
                            .build())
                    .build();

            DataStream<String> icebergStream = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "my-iceberg-source"
//                        TypeInformation.of(RowData.class)
                    )
                    .disableChaining()
                    .map(new MapFunction<RowData, String>() {
                        @Override
                        public String map(RowData rowData) {
                            return String.format("Event[id=%s]",
                                    rowData.getString(0).toString()
                                );
                        }
                    })
                    .returns(Types.STRING);
//                    .returns(TypeInformation.of(RowData.class))
                    ;
//
//            // --- 5. Consume and Process the Data ---
            icebergStream
//                    .map(new MapFunction<RowData, String>() {
//                        @Override
//                        public String map(RowData rowData) {
//                            return String.format("Event[id=%s]",
//                                    rowData.getString(0).toString()
//                            );
//                        }
//                    })
//                    .map(rowData -> "Stream Received: " + rowData.toString())
                    .print();


            // 6. Execute the Flink job
            env.execute("Iceberg Nessie MinIO DataStream Consumer");


//            TableLoader tableLoader = TableLoader.fromCatalog("s3://warehouse/:", null);
//
//
//
//            Configuration hadoopConf = new Configuration();
////            hadoopConf.set("hive.metastore.uris", "thrift://localhost:9083");
//
//
//            TableLoader tableLoader = TableLoader.fromCatalog(null, null);
////            TableLoader tableLoader = TableLoader.fromCatalog(
////                    hadoopConf,
////                    "hive_catalog", // Catalog Name
////                    "nyc.taxis" // Full table identifier
////            );
//
//            DataStream<RowData> sourceStream = FlinkSource.forRowData()
//                    .tableLoader(tableLoader)
//                    .streaming(true)
//                    .build();
//
//            sourceStream.print();
//
////// Example Write:
////            FlinkSink.forRowData(outputStream)
////                    .tableLoader(tableLoader)
////                    .append();
//
//
//
//            EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
//            TableEnvironment tEnv = TableEnvironment.create(settings);
//
////            Hive SQL client:
////            DESCRIBE FORMATTED nyc.taxis;
//            String catalogSql = String.join("\n",
//                    "CREATE CATALOG hive_catalog WITH (",
//                    "  'type'='iceberg',",
//                    "  'catalog-type'='hive',",
//                    "  'uri'='thrift://localhost:9083',",
//                    "  'warehouse'='file:/opt/hive/data/warehouse'",
//                    ");"
//            );
//
//            tEnv.executeSql(catalogSql);
//
//            tEnv.executeSql("USE CATALOG hive_catalog;");
//            tEnv.executeSql("SELECT * FROM my_iceberg_table;");
////
//////        System.setProperty("fs.s3a.endpoint", "http://storage:9000");
////            System.setProperty("fs.s3a.endpoint", "http://localhost:9001");
////            System.setProperty("fs.s3a.access.key", "admin");
////            System.setProperty("fs.s3a.secret.key", "password");
////            System.setProperty("fs.s3a.path.style.access", "true");
////
////            // set up the execution environment
////            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////
////
////            // set up the table environment
////            final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
////                    env,
////                    EnvironmentSettings.newInstance().inStreamingMode().build());
////
////            // create the Nessie catalog
////            tableEnv.executeSql(
////                    "CREATE CATALOG iceberg WITH ("
////                            + "'type'='iceberg',"
////                            + "'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
////                            + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
////                            + "'uri'='http://localhost:19120/api/v1',"
////                            + "'authentication.type'='none',"
////                            + "'ref'='main',"
////                            + "'client.assume-role.region'='us-east-1',"
////                            + "'warehouse' = 's3://warehouse',"
////                            + "'s3.endpoint'='http://localhost:9002',"
////                            + "'s3.access.key'='admin',"
////                            + "'s3.secret.key'='password'"
////                            + ")");
////
////            // List all catalogs
////            TableResult result = tableEnv.executeSql("SHOW CATALOGS");
////
////            // Print the result to standard out
////            result.print();
////
////
////
////
//





//
//		Map<String, String> config = new HashMap<>();
//		config.put("host", "localhost");
//		config.put("port", "9001");
//
//		// Set up Flink configuration to expose the Web UI
//		Configuration flinkConfig = new Configuration();
//		flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//
//		// Use LocalStreamEnvironment to enable the Flink Web UI
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
//		final ParameterTool parameterTool = ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromMap(config));
//		env.getConfig().setGlobalJobParameters(parameterTool);
//
//
//		// KafkaSource (new API)
//		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//				.setBootstrapServers("localhost:9092")
//				.setTopics("test1")
//				.setGroupId("flink-group-1")
//				.setStartingOffsets(OffsetsInitializer.latest())
//				.setValueOnlyDeserializer(new SimpleStringSchema())
//				.build();
//
//		// Add Kafka source using new API
//		DataStreamSource<String> kafkaStreamSource = env.fromSource(
//				kafkaSource,
//				WatermarkStrategy.noWatermarks(),
//				"Kafka-Source"
//		);
//
//		DataStream<Tuple3<Integer, String, String>> kafkaStream = kafkaStreamSource
//				.map(line -> {
//						String[] parts = line.split(",");
//						Integer col1 = Integer.parseInt(parts[0]);
//						String col2 = parts[1];
//						String col3 = parts[2];
//						return new Tuple3<>(col1, col2, col3);
//					})
//				.returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));
//
//
//		Schema schema = Schema.newBuilder()
////				.primaryKey("f0")
//				.column("column1", "INT")
//				.column("column2", "STRING")
//				.column("column3", "STRING")
//				.build();
//
//		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
////		tableEnv.registerDataStream("KafkaSource", kafkaStream);
//
//		Table t1 = tableEnv.fromDataStream(kafkaStream)
//				.select($("*"))
//				.where($("f1").isEqual("A"))
//				;
////		Table t1 = tableEnv.sqlQuery("SELECT * FROM KafkaSource WHERE f1 = 'A'");
//
//		t1.execute().print();
	}
}
