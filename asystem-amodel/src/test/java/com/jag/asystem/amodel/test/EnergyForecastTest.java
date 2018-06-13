package com.jag.asystem.amodel.test;

import static com.cloudera.framework.common.Driver.CONF_CLDR_JOB_METADATA;
import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.common.Driver.getApplicationProperty;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.DfsServer.Runtime;
import com.cloudera.framework.testing.server.PythonServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import com.jag.asystem.amodel.Counter;
import com.jag.asystem.amodel.EnergyForecastInterday;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class EnergyForecastTest implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance(Runtime.CLUSTER_DFS);

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  @ClassRule
  public static final PythonServer pythonServer = PythonServer.getInstance();

  public final TestMetaData testMetaDataPristine = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}}).dataSetLabels(new String[][][]{{{"pristine"}}})
    .dataSetDestinationDirs(DATASET_DIR_ASTORE).asserts(ImmutableMap.of(EnergyForecastInterday.class.getName(), ImmutableMap.of(
      Counter.TRAINING_INSTANCES, 30L,
      Counter.TESTING_INSTANCES, 11L
    )));

  @TestWith({"testMetaDataPristine"})
  public void testEnergyForecastIntraDay(TestMetaData testMetaData) throws Exception {
    assertEquals(0, pythonServer.execute(ABS_DIR_PYTHON_BIN, new File(ABS_DIR_PYTHON_SRC, "energyforecast_intraday.py"),
      Arrays.asList(DATASET_DIR_ASTORE, DATASET_ABS_AMODEL_ENERGYFORECAST_INTRADAY, DATASET_TMP_AMODEL_ENERGYFORECAST_INTRADAY)));
  }

  @TestWith({"testMetaDataPristine"})
  public void testEnergyForecastInterDay(TestMetaData testMetaData) throws Exception {
    EnergyForecastInterday driver = new EnergyForecastInterday(dfsServer.getConf());
    driver.getConf().set(Driver.CONF_CLDR_JOB_GROUP, "test-asystem-amodel-energyforecast");
    driver.getConf().set(Driver.CONF_CLDR_JOB_NAME, "test-asystem-amodel-energyforecast-validation");
    driver.getConf().set(Driver.CONF_CLDR_JOB_VERSION, getApplicationProperty("APP_VERSION"));
    driver.getConf().set(CONF_CLDR_JOB_METADATA, "true");
    driver.pollMetaData(driver.getMetaData(0, null, null));
    assertEquals(SUCCESS, driver.runner(
      dfsServer.getPath(DATASET_DIR_ASTORE).toString(), dfsServer.getPath(DATASET_DIR_AMODEL_ENERGYFORECAST_INTERDAY).toString()));
    assertCounterEquals(testMetaData, driver.getCounters());
    assertEquals(0, pythonServer.execute(ABS_DIR_PYTHON_BIN, new File(ABS_DIR_PYTHON_SRC, "energyforecast_interday.py"),
      Arrays.asList(DATASET_DIR_AMODEL_ENERGYFORECAST_INTERDAY, DATASET_ABS_AMODEL_ENERGYFORECAST_INTERDAY,
        DATASET_TMP_AMODEL_ENERGYFORECAST_INTERDAY)));
  }

  private static final String DATASET_DIR_ASTORE = "/data/asystem-astore";
  private static final String DATASET_REL_AMODEL_ENERGYFORECAST_INTERDAY =
    "/asystem-amodel/asystem/amodel/energyforecastinterday";
  private static final String DATASET_DIR_AMODEL_ENERGYFORECAST_INTERDAY =
    "/data" + DATASET_REL_AMODEL_ENERGYFORECAST_INTERDAY;
  private static final String DATASET_ABS_AMODEL_ENERGYFORECAST_INTERDAY =
    "file://" + ABS_DIR_TARGET + DATASET_REL_AMODEL_ENERGYFORECAST_INTERDAY;
  private static final String DATASET_TMP_AMODEL_ENERGYFORECAST_INTERDAY =
    ABS_DIR_TARGET + "/asystem-amodel-tmp/asystem/amodel/energyforecastinterday";

  private static final String DATASET_ABS_AMODEL_ENERGYFORECAST_INTRADAY =
    DATASET_ABS_AMODEL_ENERGYFORECAST_INTERDAY.replace("interday", "intraday");
  private static final String DATASET_TMP_AMODEL_ENERGYFORECAST_INTRADAY =
    DATASET_TMP_AMODEL_ENERGYFORECAST_INTERDAY.replace("interday", "intraday");

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
