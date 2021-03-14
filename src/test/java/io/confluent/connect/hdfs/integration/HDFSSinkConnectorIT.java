package io.confluent.connect.hdfs.integration;

import static io.confluent.connect.hdfs.HdfsSinkConnectorConfig.HDFS_URL_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.hdfs.HdfsSinkConnector;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSSinkConnectorIT extends BaseConnectorIT{

  private static final Logger log = LoggerFactory.getLogger(HDFSSinkConnectorIT.class);
  // connector and test configs
  private static final String CONNECTOR_NAME = "hdfs-sink";
  private static final String TEST_TOPIC_NAME = "TestTopic";
  private static final int NUM_RECORDS_INSERT = 100;
  //hdfs cluster configs
  private static final int HDFS_PORT = 9001;
  private static final int NUM_DATANODES = 3;
  //topic configs
  private static final List<String> KAFKA_TOPICS = Collections.singletonList(TEST_TOPIC_NAME);
  private static final int DEFAULT_TOPIC_PARTITION = 0;

  private JsonConverter jsonConverter;
  private Producer<byte[], byte[]> producer;

  private MiniDFSCluster cluster;

  @Before
  public void before() throws IOException {
    initializeJsonConverter();
    initializeCustomProducer();
    setupProperties();
    //start HDFS cluster
    Configuration localConf = new Configuration();
    cluster = createDFSCluster(localConf);
    //add class specific props
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(FLUSH_SIZE_CONFIG, "3");
    props.put(HDFS_URL_CONFIG, "hdfs://" + cluster.getNameNode().getClientNamenodeAddress());

    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
  }

  @After
  public void after() throws IOException {
    cluster.getFileSystem().close();
    cluster.shutdown();
  }

  @Test
  public void testAvroRecrodsWritten() throws Exception {
    // start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, Math.min(KAFKA_TOPICS.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);
    SinkRecord sampleRecord = getSampleRecord(recordValueSchema, recordValueStruct);
    // Send records to Kafka
    produceRecordsNoHeaders(NUM_RECORDS_INSERT, sampleRecord);

    log.info("Waiting for files in HDFS...");
//    int expectedFileCount = NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD;
//    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFileCount);
//
//    List<String> expectedFilenames = getExpectedFilenames(TEST_TOPIC_NAME, TOPIC_PARTITION,
//        FLUSH_SIZE_STANDARD, NUM_RECORDS_INSERT, expectedFileExtension);
//    assertTrue(fileNamesValid(TEST_BUCKET_NAME, expectedFilenames));
//    assertTrue(fileContentsAsExpected(TEST_BUCKET_NAME, FLUSH_SIZE_STANDARD, recordValueStruct));
  }

  //TODO: integration tests to verify tmp file and WAL behavior, tests with hive,
  // tests with different partitioners

  private void produceRecordsNoHeaders(int recordCount, SinkRecord record)
      throws ExecutionException, InterruptedException {
    produceRecords(record.topic(), recordCount, record, true, true, false);
  }

  private void produceRecords(
      String topic,
      int recordCount,
      SinkRecord record,
      boolean withKey,
      boolean withValue,
      boolean withHeaders
  ) throws ExecutionException, InterruptedException {
    byte[] kafkaKey = null;
    byte[] kafkaValue = null;
    Iterable<Header> headers = Collections.emptyList();
    if (withKey) {
      kafkaKey = jsonConverter.fromConnectData(topic, Schema.STRING_SCHEMA, record.key());
    }
    if (withValue) {
      kafkaValue = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    }
    if (withHeaders) {
      headers = sampleHeaders();
    }
    ProducerRecord<byte[],byte[]> producerRecord =
        new ProducerRecord<>(topic, DEFAULT_TOPIC_PARTITION, kafkaKey, kafkaValue, headers);
    for (long i = 0; i < recordCount; i++) {
      producer.send(producerRecord).get();
    }
  }

  private void setupProperties() {
    props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, HdfsSinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
  }

  private void initializeJsonConverter() {
    Map<String, Object> jsonConverterProps = new HashMap<>();
    jsonConverterProps.put("schemas.enable", "true");
    jsonConverterProps.put("converter.type", "value");
    jsonConverter = new JsonConverter();
    jsonConverter.configure(jsonConverterProps);
  }

  private SinkRecord getSampleRecord(Schema recordValueSchema, Struct recordValueStruct ) {
    return new SinkRecord(
        TEST_TOPIC_NAME,
        DEFAULT_TOPIC_PARTITION,
        Schema.STRING_SCHEMA,
        "key",
        recordValueSchema,
        recordValueStruct,
        0
    );
  }

  private Iterable<Header> sampleHeaders() {
    return Arrays.asList(
        new RecordHeader("first-header-key", "first-header-value".getBytes()),
        new RecordHeader("second-header-key", "second-header-value".getBytes())
    );
  }

  private Schema getSampleStructSchema() {
    return SchemaBuilder.struct()
        .field("ID", Schema.INT64_SCHEMA)
        .field("myBool", Schema.BOOLEAN_SCHEMA)
        .field("myInt32", Schema.INT32_SCHEMA)
        .field("myFloat32", Schema.FLOAT32_SCHEMA)
        .field("myFloat64", Schema.FLOAT64_SCHEMA)
        .field("myString", Schema.STRING_SCHEMA)
        .build();
  }

  private Struct getSampleStructVal(Schema structSchema) {
    Date sampleDate = new Date(1111111);
    sampleDate.setTime(0);
    return new Struct(structSchema)
        .put("ID", (long) 1)
        .put("myBool", true)
        .put("myInt32", 32)
        .put("myFloat32", 3.2f)
        .put("myFloat64", 64.64)
        .put("myString", "theStringVal");
  }

  private void initializeCustomProducer() {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    producer = new KafkaProducer<>(producerProps);
  }

  private MiniDFSCluster createDFSCluster(Configuration conf) throws IOException {
    MiniDFSCluster cluster;
    String[] hosts = {"localhost", "localhost", "localhost"};
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.hosts(hosts).nameNodePort(HDFS_PORT).numDataNodes(NUM_DATANODES);
    cluster = builder.build();
    cluster.waitActive();
    return cluster;
  }
}
