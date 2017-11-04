package com.jag.asystem.amodel.test;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.testing.server.DfsServer.Runtime;
import com.cloudera.framework.testing.server.PythonServer;
import com.jag.asystem.amodel.DatumFactory;
import com.jag.asystem.amodel.EnergyDriver;

import static com.cloudera.framework.common.Driver.Counter.RECORDS_IN;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_OUT;
import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterGreaterThan;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class EnergyTest implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance(Runtime.CLUSTER_DFS);

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  @ClassRule
  public static final PythonServer pythonServer = PythonServer.getInstance();

  public final TestMetaData testMetaDataPristine = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}}).dataSetLabels(new String[][][]{{{"pristine"}}})
    .dataSetDestinationDirs(DATASET_DIR_ASTORE).asserts(ImmutableMap.of(EnergyDriver.class.getName(), ImmutableMap.of(
      RECORDS_IN, 340000L - 1,
      RECORDS_OUT, 6L - 1
    )));

  @TestWith({"testMetaDataPristine"})
  public void testEnergy(TestMetaData testMetaData) throws Exception {
    EnergyDriver driver = new EnergyDriver(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(
      dfsServer.getPath(DATASET_DIR_ASTORE).toString(), dfsServer.getPath(DATASET_DIR_AMODEL).toString()));
    assertCounterGreaterThan(testMetaData, driver.getCounters());
    assertEquals(0, pythonServer.execute(ABS_DIR_PYTHON_BIN, new File(ABS_DIR_PYTHON_SRC, "energy.py"),
      Arrays.asList(DATASET_DIR_AMODEL, DATASET_DIR_MODEL)));
  }

  private static final String DATASET_DIR_ASTORE = "/data/asystem-astore";
  private static final String DATASET_DIR_AMODEL = "/data/asystem-amodel/asystem/" +
    Driver.getApplicationProperty("APP_VERSION") + "/amodel/" + DatumFactory.getModelProperty("MODEL_VERSION") + "/energy";
  private static final String DATASET_DIR_MODEL = ABS_DIR_TARGET + "/asystem-amodel/asystem/" +
    Driver.getApplicationProperty("APP_VERSION") + "/amodel/" + DatumFactory.getModelProperty("MODEL_VERSION") + "/energy";

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
