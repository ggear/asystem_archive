package com.jag.asystem.amodel.test;

import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

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
import com.jag.asystem.amodel.EnergyForecastDay;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

@RunWith(TestRunner.class)
public class DatasetTest implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance(Runtime.CLUSTER_DFS);

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  @ClassRule
  public static final PythonServer pythonServer = PythonServer.getInstance();

  public final TestMetaData testMetaDataPristine = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET).dataSetNames("astore")
    .dataSetSubsets(new String[][]{{"datums"}}).dataSetLabels(new String[][][]{{{"pristine"}}}).dataSetDestinationDirs(DATASET_DIR_ASTORE);

  @TestWith({"testMetaDataPristine"})
  public void testEnergyForecast(TestMetaData testMetaData) throws Exception {
    assertEquals(0, pythonServer.execute(ABS_DIR_PYTHON_BIN, new File(ABS_DIR_PYTHON_SRC, "dataset.py"),
      Arrays.asList(DATASET_DIR_ASTORE)));
  }

  private static final String DATASET_DIR_ASTORE = "/data/asystem-astore";

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
