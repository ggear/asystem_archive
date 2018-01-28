package com.jag.asystem.arouter.test;

import static com.jag.asystem.arouter.Constants.FLUME_AGENT;
import static com.jag.asystem.arouter.Constants.FLUME_CONFIG;
import static com.jag.asystem.arouter.Constants.HDFS_DIR;
import static com.jag.asystem.arouter.Constants.MODEL_1_SINK;
import static com.jag.asystem.arouter.Constants.MODEL_1_SOURCE;
import static com.jag.asystem.arouter.Constants.MQTT_TOPIC;
import static com.jag.asystem.arouter.Constants.getFlumeEnv;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.cloudera.framework.common.flume.MqttSource;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MqttServer;
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
public class FlumeTest implements TestConstants {

  @ClassRule
  public static final MqttServer mqttServer = MqttServer.getInstance();

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final FlumeServer flumeServer = FlumeServer.getInstance();

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance(HiveServer.Runtime.LOCAL_SPARK);

  private static final DatumFactory DATUM_FACTORY = new DatumFactory();

  private static final Logger LOG = LoggerFactory.getLogger(FlumeTest.class);

  private static final int DATUMS_COUNT = 16;

  private MqttClient client;

  @Before
  public void mqttConnect() throws MqttException {
    client = new MqttClient(mqttServer.getConnectString(), UUID.randomUUID().toString(), new MemoryPersistence());
    client.connect();
  }

  @After
  public void mqttDisconnect() throws MqttException {
    client.disconnect();
  }

  @SuppressWarnings("Duplicates")
  @Test
  public void testFlumePipeline() throws Exception {
    assertTrue(flumeServer.crankPipeline(
      getFlumeEnv(ABS_DIR_FLUME, dfsServer.getPathUri(HDFS_DIR)), FLUME_CONFIG, emptyMap(), emptyMap(),
      FLUME_AGENT, MODEL_1_SOURCE, MODEL_1_SINK, new MqttSource(), new HDFSEventSink(),
      HDFS_DIR, DATUMS_COUNT, this::mqttClientSendMessage) > 0);
    Set<String> partitions = new HashSet<>();
    for (Path path : dfsServer.listFilesDfs(HDFS_DIR)) {
      String pathContents = dfsServer.readFileAsString(path);
      if (LOG.isInfoEnabled()) {
        LOG.info("FlumeTest sink has written file [" + path.getName() + "] with size [" + pathContents.length() + "] bytes");
      }
      if (partitions.add(path.getParent().getParent().getParent().getParent().getParent().getParent().getParent().toString())) {
        hiveServer.execute(
          "CREATE EXTERNAL TABLE datum_" + (partitions.size() - 1) + " " +
            "PARTITIONED BY (" +
            "arouter_version STRING, arouter_id STRING, arouter_ingest BIGINT, " +
            "arouter_start BIGINT, arouter_finish BIGINT, arouter_model INT" +
            ") STORED AS AVRO " +
            "LOCATION '" + path.toString().substring(0, path.toString().indexOf("/arouter_version")) + "' " +
            "TBLPROPERTIES ('avro.schema.url'='" + FlumeTest.class.getResource("/avro/" +
            DatumFactory.getModelProperty("MODEL_VERSION") + "/datum.avsc").toString() + "') "
        );
        hiveServer.execute("MSCK REPAIR TABLE datum_" + (partitions.size() - 1));
      }
    }
    StringBuilder query = new StringBuilder();
    for (int index = 0; index < partitions.size(); index++) {
      if (partitions.size() == 1) {
        query.append("SELECT bin_timestamp, data_metric FROM datum_0 ORDER BY data_metric");
      } else {
        if (index == 0) {
          query.append("SELECT bin_timestamp, data_metric FROM ( SELECT * FROM datum_").append(index);
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
      Thread.sleep(1000);
      client.publish(MQTT_TOPIC, DATUM_FACTORY.serialize(DatumFactory.getDatumIndexed(iteration)), 0, false);
    } catch (Exception e) {
      throw new RuntimeException("Could not publish message", e);
    }
    return 1;
  }

}
