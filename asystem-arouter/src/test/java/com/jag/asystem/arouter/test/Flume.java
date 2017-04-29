package com.jag.asystem.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import com.cloudera.framework.common.flume.MqttSource;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.cloudera.framework.testing.server.MqttServer;
import com.google.common.collect.ImmutableMap.Builder;
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

@RunWith(TestRunner.class)
public class Flume implements TestConstants {

  @ClassRule
  public static final MqttServer mqttServer = MqttServer.getInstance();

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final FlumeServer flumeServer = FlumeServer.getInstance();

  private final Map<String, String> FLUME_SUBSTITUTIONS =
    new Builder<String, String>()
      .put("FLUME_AGENT_NAME", "arouter")
      .put("MQTT_USER_NAME", "admin")
      .put("MQTT_PASSWORD_FILE", ABS_DIR_CLASSES_TEST + "/.mqtt_broker")
      .put("MQTT_BROKER_HOST", "localhost")
      .put("MQTT_BROKER_PORT", "1883")
      .put("MQTT_TOPIC_NAME", "asystem/datum")
      .put("MQTT_BACK_OFF", "100")
      .put("MQTT_MAX_BACK_OFF", "100")
      .put("FLUME_MQTT_CHECKPOINT_DIR", ABS_DIR_FLUME + "/file_channel/checkpoint")
      .put("FLUME_MQTT_DATA_DIRS", ABS_DIR_FLUME + "/file_channel/data")
      .put("ROOT_HDFS", dfsServer.getPathUri("/"))
      .put("ROOT_DIR_HDFS", "/asystem")
      .build();

  private MqttClient client;

  @Before
  public void mqttClientConnect() throws MqttException {
    client = new MqttClient(mqttServer.getConnectString(), UUID.randomUUID().toString(), new MemoryPersistence());
    client.connect();
  }

  private void mqttClientSendMessage(Integer iteration) {
    try {
      client.publish(FLUME_SUBSTITUTIONS.get("MQTT_TOPIC_NAME"), UUID.randomUUID().toString().getBytes(), 0, false);
    } catch (MqttException e) {
      throw new RuntimeException("Could not publish message", e);
    }
  }

  @After
  public void mqttClientDisconnect() throws MqttException {
    client.disconnect();
  }

  @Test
  public void testFlume() throws MqttException, InterruptedException, IOException, EventDeliveryException {
    assertEquals(1,
      flumeServer.crankPipeline(FLUME_SUBSTITUTIONS,
        "flume/flume-conf.properties", Collections.emptyMap(), Collections.emptyMap(),
        FLUME_SUBSTITUTIONS.get("FLUME_AGENT_NAME"), "mqtt", "s3",
        new MqttSource(), new HDFSEventSink(), FLUME_SUBSTITUTIONS.get("ROOT_DIR_HDFS"), 10, iteration -> mqttClientSendMessage(iteration)));
  }

}
