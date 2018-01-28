package com.jag.asystem.astore.test;

import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static com.jag.asystem.arouter.Constants.FLUME_AGENT;
import static com.jag.asystem.arouter.Constants.FLUME_CONFIG;
import static com.jag.asystem.arouter.Constants.HDFS_DIR;
import static com.jag.asystem.arouter.Constants.MODEL_1_SINK;
import static com.jag.asystem.arouter.Constants.MODEL_1_SOURCE;
import static com.jag.asystem.arouter.Constants.MQTT_TOPIC;
import static com.jag.asystem.arouter.Constants.getFlumeEnv;
import static com.jag.asystem.astore.Counter.DATUMS_PROCESSED_COUNT;
import static com.jag.asystem.astore.Counter.FILES_PROCESSED_DONE;
import static com.jag.asystem.astore.Counter.FILES_PROCESSED_REDO;
import static com.jag.asystem.astore.Counter.FILES_PROCESSED_SKIP;
import static com.jag.asystem.astore.Counter.FILES_STAGED_DONE;
import static com.jag.asystem.astore.Counter.FILES_STAGED_REDO;
import static com.jag.asystem.astore.Counter.FILES_STAGED_SKIP;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.common.flume.MqttSource;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.cloudera.framework.testing.server.HiveServer;
import com.cloudera.framework.testing.server.HiveServer.Runtime;
import com.cloudera.framework.testing.server.MqttServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import com.jag.asystem.amodel.DatumFactory;
import com.jag.asystem.astore.Process;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.hadoop.fs.Path;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(TestRunner.class)
public class ProcessTest implements TestConstants {

  @ClassRule
  public static final MqttServer mqttServer = MqttServer.getInstance();

  @ClassRule
  public static final FlumeServer flumeServer = FlumeServer.getInstance();

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  @ClassRule
  public static final HiveServer hiveServer = HiveServer.getInstance(Runtime.LOCAL_MR2);

  public final TestMetaData testPristine = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}})
    .dataSetLabels(new String[][][]{{{"pristine"}}})
    .parameters(ImmutableMap.of(DATA_GENERATE, Boolean.FALSE))
    .dataSetDestinationDirs(HDFS_DIR)
    .asserts(ImmutableMap.of(Process.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, 0L)
      .put(FILES_STAGED_REDO, 0L)
      .put(FILES_STAGED_DONE, 18L)
      .put(FILES_PROCESSED_SKIP, 0L)
      .put(FILES_PROCESSED_REDO, 0L)
      .put(FILES_PROCESSED_DONE, 49L)
      .put(DATUMS_PROCESSED_COUNT, 100L)
      .build())
    );

  public final TestMetaData testOverlap = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}})
    .dataSetLabels(new String[][][]{{{"overlap"}}})
    .parameters(ImmutableMap.of(DATA_GENERATE, Boolean.FALSE))
    .dataSetDestinationDirs(HDFS_DIR)
    .asserts(ImmutableMap.of(Process.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, 1L)
      .put(FILES_STAGED_REDO, 1L)
      .put(FILES_STAGED_DONE, 1L)
      .put(FILES_PROCESSED_SKIP, 1L)
      .put(FILES_PROCESSED_REDO, 1L)
      .put(FILES_PROCESSED_DONE, 24L)
      .put(DATUMS_PROCESSED_COUNT, 100L)
      .build())
    );

  public final TestMetaData testCorrupt = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}})
    .dataSetLabels(new String[][][]{{{"corrupt"}}})
    .parameters(ImmutableMap.of(DATA_GENERATE, Boolean.FALSE))
    .dataSetDestinationDirs(HDFS_DIR)
    .asserts(ImmutableMap.of(Process.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, 0L)
      .put(FILES_STAGED_REDO, 0L)
      .put(FILES_STAGED_DONE, 0L)
      .put(FILES_PROCESSED_SKIP, 0L)
      .put(FILES_PROCESSED_REDO, 0L)
      .put(FILES_PROCESSED_DONE, 0L)
      .put(DATUMS_PROCESSED_COUNT, 100L)
      .build())
    );

  public final TestMetaData testEmpty = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}})
    .dataSetLabels(new String[][][]{{{"empty"}}})
    .parameters(ImmutableMap.of(DATA_GENERATE, Boolean.FALSE))
    .dataSetDestinationDirs(HDFS_DIR)
    .asserts(ImmutableMap.of(Process.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, 0L)
      .put(FILES_STAGED_REDO, 0L)
      .put(FILES_STAGED_DONE, 0L)
      .put(FILES_PROCESSED_SKIP, 0L)
      .put(FILES_PROCESSED_REDO, 0L)
      .put(FILES_PROCESSED_DONE, 0L)
      .put(DATUMS_PROCESSED_COUNT, 0L)
      .build())
    );

  public final TestMetaData testRewrite = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}})
    .dataSetLabels(new String[][][]{{{"rewrite"}}})
    .parameters(ImmutableMap.of(DATA_GENERATE, Boolean.FALSE))
    .dataSetDestinationDirs(HDFS_DIR)
    .asserts(ImmutableMap.of(Process.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, 0L)
      .put(FILES_STAGED_REDO, 0L)
      .put(FILES_STAGED_DONE, 0L)
      .put(FILES_PROCESSED_SKIP, 0L)
      .put(FILES_PROCESSED_REDO, 0L)
      .put(FILES_PROCESSED_DONE, 0L)
      .put(DATUMS_PROCESSED_COUNT, 0L)
      .build())
    );

  public final TestMetaData testAll = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}})
    .dataSetLabels(new String[][][]{{{null}}})
    .parameters(ImmutableMap.of(DATA_GENERATE, Boolean.FALSE))
    .dataSetDestinationDirs(HDFS_DIR)
    .asserts(ImmutableMap.of(Process.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, 0L)
      .put(FILES_STAGED_REDO, 2L)
      .put(FILES_STAGED_DONE, 19L)
      .put(FILES_PROCESSED_SKIP, 0L)
      .put(FILES_PROCESSED_REDO, 2L)
      .put(FILES_PROCESSED_DONE, 73L)
      .put(DATUMS_PROCESSED_COUNT, 100L)
      .build())
    );

  public final TestMetaData testGenerated = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}})
    .dataSetLabels(new String[][][]{{{"empty"}}})
    .parameters(ImmutableMap.of(DATA_GENERATE, Boolean.TRUE))
    .dataSetDestinationDirs(HDFS_DIR)
    .asserts(ImmutableMap.of(Process.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, 0L)
      .put(FILES_STAGED_REDO, 0L)
      .put(FILES_STAGED_DONE, 16L)
      .put(FILES_PROCESSED_SKIP, 0L)
      .put(FILES_PROCESSED_REDO, 0L)
      .put(FILES_PROCESSED_DONE, 2L)
      .put(DATUMS_PROCESSED_COUNT, 16L)
      .build())
    );

  private static final DatumFactory DATUM_FACTORY = new DatumFactory();

  private static final Logger LOG = LoggerFactory.getLogger(ProcessTest.class);

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
  @TestWith({"testPristine", "testOverlap", "testCorrupt", "testEmpty", "testRewrite", "testAll", "testGenerated"})
  public void testProcess(TestMetaData test) throws Exception {
    if (test.<Boolean>getParameter(DATA_GENERATE)) {
      assertTrue(flumeServer.crankPipeline(
        getFlumeEnv(ABS_DIR_FLUME, dfsServer.getPathUri(HDFS_DIR)), FLUME_CONFIG, emptyMap(), emptyMap(),
        FLUME_AGENT, MODEL_1_SOURCE, MODEL_1_SINK, new MqttSource(), new HDFSEventSink(),
        HDFS_DIR, DATUMS_COUNT, this::mqttClientSendMessage) > 0);
    }
    Driver driver = new Process(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(HDFS_DIR).toString()));
    assertCounterEquals(test, driver.getCounters());
    Set<String> partitions = new HashSet<>();
    for (Path path : dfsServer.listFilesDfs(HDFS_DIR)) {
      if (path.toString().endsWith(".parquet") &&
        partitions.add(path.getParent().getParent().getParent().getParent().getParent().getParent().toString())) {
        hiveServer.execute(
          "CREATE TABLE datum_" + (partitions.size() - 1) + "_avro " +
            "PARTITIONED BY (astore_version STRING, astore_year STRING, astore_month STRING, astore_model STRING, astore_metric STRING) " +
            "STORED AS AVRO " +
            "TBLPROPERTIES ('avro.schema.url'='" + ProcessTest.class.getResource("/avro/" +
            DatumFactory.getModelProperty("MODEL_VERSION") + "/datum.avsc").toString() + "') "
        );
        hiveServer.execute(
          "CREATE EXTERNAL TABLE datum_" + (partitions.size() - 1) + " LIKE datum_" + (partitions.size() - 1) + "_avro " +
            "STORED AS PARQUET " + "" +
            "LOCATION '" + path.toString().substring(0, path.toString().indexOf("/astore_version")) + "'"
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
    assertEquals(test.<Long>getAssert(DATUMS_PROCESSED_COUNT),
      query.length() > 0 ? new Long(hiveServer.execute(query.toString()).size()) : new Long(0));
    driver.reset();
    assertEquals(SUCCESS, driver.runner(
      dfsServer.getPath(HDFS_DIR).toString()));
    assertCounterEquals(ImmutableMap.of(Process.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, test.<Long>getAssert(FILES_STAGED_SKIP) +
        test.<Long>getAssert(FILES_STAGED_REDO) + test.<Long>getAssert(FILES_STAGED_DONE))
      .put(FILES_STAGED_REDO, 0L)
      .put(FILES_STAGED_DONE, 0L)
      .put(FILES_PROCESSED_REDO, 0L)
      .put(FILES_PROCESSED_SKIP, test.<Long>getAssert(FILES_PROCESSED_SKIP) + test.<Long>getAssert(FILES_PROCESSED_DONE))
      .put(FILES_PROCESSED_DONE, 0L)
      .build()), driver.getCounters());
  }

  private static final String DATA_GENERATE = "DATA_GENERATE";

  @SuppressWarnings("SameReturnValue")
  private int mqttClientSendMessage(Integer iteration) {
    try {
      client.publish(MQTT_TOPIC, DATUM_FACTORY.serialize(DatumFactory.getDatumIndexed(iteration)), 0, false);
    } catch (MqttException e) {
      throw new RuntimeException("Could not publish message", e);
    }
    return 1;
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
