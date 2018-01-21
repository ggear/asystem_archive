package com.jag.asystem.amodel.test;

import static com.cloudera.framework.common.Driver.Counter.RECORDS_IN;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_OUT;
import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static com.cloudera.framework.testing.Assert.assertCounterGreaterThan;
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
import com.jag.asystem.amodel.DatumFactory;
import com.jag.asystem.amodel.EnergyForecastPreparation;
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
    .dataSetDestinationDirs(DATASET_DIR_ASTORE).asserts(ImmutableMap.of(EnergyForecastPreparation.class.getName(), ImmutableMap.of(
      RECORDS_OUT, 81L
    )));

  @TestWith({"testMetaDataPristine"})
  public void testEnergyForecast(TestMetaData testMetaData) throws Exception {
    EnergyForecastPreparation driver = new EnergyForecastPreparation(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(
      dfsServer.getPath(DATASET_DIR_ASTORE).toString(), dfsServer.getPath(DATASET_DIR_AMODEL).toString()));
    assertCounterEquals(testMetaData, driver.getCounters());
    assertEquals(0, pythonServer.execute(ABS_DIR_PYTHON_BIN, new File(ABS_DIR_PYTHON_SRC, "energyforecast.py"),
      Arrays.asList(DATASET_DIR_AMODEL, DATASET_ABS_AMODEL, DATASET_TMP_AMODEL)));
  }

  private static final String DATASET_DIR_ASTORE = "/data/asystem-astore";
  private static final String DATASET_DIR_AMODEL = "/data/asystem-amodel/asystem/" +
    Driver.getApplicationProperty("APP_VERSION") + "/amodel/" + DatumFactory.getModelProperty("MODEL_ENERGYFORECAST_VERSION")
    + "/energyforecast";
  private static final String DATASET_ABS_AMODEL = "file://" + ABS_DIR_TARGET + "/asystem-amodel/asystem/" +
    Driver.getApplicationProperty("APP_VERSION") + "/amodel/" + DatumFactory.getModelProperty("MODEL_ENERGYFORECAST_VERSION")
    + "/energyforecast";
  private static final String DATASET_TMP_AMODEL = ABS_DIR_TARGET + "/asystem-amodel-tmp/asystem/" +
    Driver.getApplicationProperty("APP_VERSION") + "/amodel/" + DatumFactory.getModelProperty("MODEL_ENERGYFORECAST_VERSION")
    + "/energyforecast";

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
