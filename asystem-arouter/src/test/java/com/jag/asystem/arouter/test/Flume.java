package com.jag.asystem.arouter.test;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.common.flume.MqttSource;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MqttServer;
import com.google.common.collect.ImmutableMap.Builder;
import com.jag.asystem.amodel.DatumFactory;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.hadoop.fs.Path;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(TestRunner.class)
public class Flume implements TestConstants {

  @ClassRule
  public static final MqttServer mqttServer = MqttServer.getInstance();

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final FlumeServer flumeServer = FlumeServer.getInstance();

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance(HiveServer.Runtime.LOCAL_SPARK);

  private static final DatumFactory DATUM_FACTORY = new DatumFactory();

  private static final Logger LOG = LoggerFactory.getLogger(Flume.class);

  private static final String HDFS_DIR = "/data";

  private static final String FLUME_CONFIG = "flume/flume-conf.properties";

  private static final String FLUME_AGENT = "agent";

  private static final String MODEL_1_SINK = "s3_model_1";
  private static final String MODEL_1_SOURCE = "mqtt_model_1";

  private static final String MQTT_TOPIC = "asystem/" + Driver.getApplicationProperty("APP_VERSION") +
    "/anode/3C15C2C0BC90/amodel/" + DatumFactory.getModelProperty("MODEL_VERSION");

  private static final int DATUMS_COUNT = 50;

  private MqttClient client;

  private final Map<String, String> FLUME_ENV =
    new Builder<String, String>()
      .put("MQTT_BROKER_HOST", "localhost")
      .put("MQTT_BROKER_PORT", "2883")
      .put("MQTT_BATCHSIZE", "10")
      .put("HDFS_BATCHSIZE", "1")
      .put("AVRO_SCHEMA_URL", Flume.class.getResource("/avro").toString())
      .put("FLUME_MQTT_JOURNAL_DIR", ABS_DIR_FLUME + "/store/journal")
      .put("FLUME_MQTT_CHECKPOINT_DIR", ABS_DIR_FLUME + "/store/checkpoint")
      .put("FLUME_MQTT_DATA_DIRS", ABS_DIR_FLUME + "/store/data")
      .put("S3_URL", dfsServer.getPathUri(HDFS_DIR))
      .put("APP_VERSION", Driver.getApplicationProperty("APP_VERSION"))
      .build();

  @Before
  public void mqttClientConnect() throws MqttException {
    client = new MqttClient(mqttServer.getConnectString(), UUID.randomUUID().toString(), new MemoryPersistence());
    client.connect();
  }

  @After
  public void mqttClientDisconnect() throws MqttException {
    client.disconnect();
  }

  @Test
  public void testPipeline() throws Exception {
    assertTrue(flumeServer.crankPipeline(FLUME_ENV, FLUME_CONFIG, emptyMap(), emptyMap(), FLUME_AGENT,
      MODEL_1_SOURCE, MODEL_1_SINK, new MqttSource(), new HDFSEventSink(), HDFS_DIR, DATUMS_COUNT, this::mqttClientSendMessage) > 0);
    Set<String> partitions = new HashSet<>();
    for (Path path : dfsServer.listFilesDfs(HDFS_DIR)) {
      String pathContents = dfsServer.readFileAsString(path);
      if (LOG.isInfoEnabled()) {
        LOG.info("Flume sink has written file [" + path.getName() + "] with size [" + pathContents.length() + "] bytes");
      }
      if (partitions.add(path.getParent().getParent().getParent().toString())) {
        hiveServer.execute(
          "CREATE EXTERNAL TABLE datum_" + (partitions.size() - 1) + " " +
            "PARTITIONED BY (ingest_id STRING, ingest_timestamp BIGINT) " +
            "STORED AS AVRO " +
            "LOCATION '" + path.toString().substring(0, path.toString().indexOf("/ingest_id")) + "' " +
            "TBLPROPERTIES ('avro.schema.url'='" + Flume.class.getResource("/avro/" +
            DatumFactory.getModelProperty("MODEL_VERSION") + "/datum.avsc").toString() + "') "
        );
        hiveServer.execute("MSCK REPAIR TABLE datum_" + (partitions.size() - 1));
      }
    }
    StringBuilder query = new StringBuilder();
    for (int index = 0; index < partitions.size(); index++) {
      if (partitions.size() == 1) {
        query.append("SELECT data_metric FROM datum_0 ORDER BY data_metric");
      } else {
        if (index == 0) {
          query.append("SELECT data_metric FROM ( SELECT * FROM datum_").append(index);
        } else if (index == partitions.size() - 1) {
          query.append(" UNION ALL SELECT * FROM datum_").append(index).append(" ) datum ORDER BY data_metric");
        } else {
          query.append(" UNION ALL SELECT * FROM datum_").append(index);
        }
      }
    }
    assertEquals(DATUMS_COUNT, hiveServer.execute(query.toString()).size());
  }

  @SuppressWarnings("SameReturnValue")
  private int mqttClientSendMessage(Integer iteration) {
    try {
      client.publish(MQTT_TOPIC, DATUM_FACTORY.serialize(DatumFactory.getDatumIndexed(iteration)), 0, false);
    } catch (MqttException e) {
      throw new RuntimeException("Could not publish message", e);
    }
    return 1;
  }

}
