package com.jag.asystem.arouter;

import java.util.Map;

import com.cloudera.framework.common.Driver;
import com.google.common.collect.ImmutableMap.Builder;
import com.jag.asystem.amodel.DatumFactory;

public class Constants {

  public static final String FLUME_CONFIG = "flume/flume-conf.properties";

  public static final String FLUME_AGENT = "agent";

  public static final String MODEL_1_SINK = "s3_model_1";
  public static final String MODEL_1_SOURCE = "mqtt_model_1";

  public static final String HDFS_DIR = "/data/asystem-astore";

  public static final String MQTT_TOPIC = "asystem/anode/amodel/anode_version=" + Driver.getApplicationProperty("APP_VERSION") +
    "/anode_id=3C15C2C0BC90/anode_model=" + DatumFactory.getModelProperty("MODEL_VERSION");

  public static Map<String, String> getFlumeEnv(String flumeLocalWorkingDir, String flumeDfsDataDir) {
    return new Builder<String, String>()
      .put("MQTT_BROKER_HOST", "localhost")
      .put("MQTT_BROKER_PORT", "2883")
      .put("MQTT_DROP_SNAPSHOTS", "false")
      .put("MQTT_BATCHSIZE", "4")
      .put("HDFS_BATCHSIZE", "16")
      .put("AVRO_SCHEMA_URL", Constants.class.getResource("/avro").toString())
      .put("FLUME_MQTT_JOURNAL_DIR", flumeLocalWorkingDir + "/store/journal")
      .put("FLUME_MQTT_CHECKPOINT_DIR", flumeLocalWorkingDir + "/store/checkpoint")
      .put("FLUME_MQTT_DATA_DIRS", flumeLocalWorkingDir + "/store/data")
      .put("S3_URL_ASTORE", flumeDfsDataDir)
      .put("APP_VERSION", Driver.getApplicationProperty("APP_VERSION"))
      .build();
  }

}
