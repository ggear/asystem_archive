package com.jag.asystem.astore.test;

import java.io.IOException;

import com.cloudera.framework.testing.TestRunner;
import com.jag.asystem.arouter.test.Flume;
import org.apache.flume.EventDeliveryException;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class Process extends Flume {

  @Override
  public void testPipeline() throws MqttException, InterruptedException, IOException, EventDeliveryException {
    super.testPipeline();
  }

}
