package com.jag.asystem.astore.test;

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

import java.io.IOException;
import java.util.UUID;

import com.cloudera.framework.common.flume.MqttSource;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.MqttServer;
import com.jag.asystem.amodel.DatumFactory;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.hdfs.HDFSEventSink;
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
public class SchemaTest implements TestConstants {

  @ClassRule
  public static final MqttServer mqttServer = MqttServer.getInstance();

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final FlumeServer flumeServer = FlumeServer.getInstance();

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance(HiveServer.Runtime.LOCAL_SPARK);

  private static final DatumFactory DATUM_FACTORY = new DatumFactory();

  private static final Logger LOG = LoggerFactory.getLogger(SchemaTest.class);

  private static final int DATUMS_COUNT = 100;

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

  @Test
  public void testSchema() throws IOException, EventDeliveryException {
    assertTrue(flumeServer.crankPipeline(
      getFlumeEnv(ABS_DIR_FLUME, dfsServer.getPathUri(HDFS_DIR)), FLUME_CONFIG, emptyMap(), emptyMap(),
      FLUME_AGENT, MODEL_1_SOURCE, MODEL_1_SINK, new MqttSource(), new HDFSEventSink(),
      HDFS_DIR, DATUMS_COUNT, this::mqttClientSendMessage) > 0);
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
