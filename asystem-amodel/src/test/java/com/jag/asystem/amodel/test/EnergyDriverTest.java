package com.jag.asystem.amodel.test;

import com.jag.asystem.amodel.EnergyDriver;

import static com.cloudera.framework.common.Driver.Counter.RECORDS_IN;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_OUT;
import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterGreaterThan;
import static org.junit.Assert.assertEquals;

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
public class EnergyDriverTest implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  public final TestMetaData testMetaDataPristine = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}}).dataSetLabels(new String[][][]{{{"pristine"}}})
    .dataSetDestinationDirs(DATASET_DIR_INPUT).asserts(ImmutableMap.of(EnergyDriver.class.getName(), ImmutableMap.of(
      RECORDS_IN, 340000L - 1,
      RECORDS_OUT, 6L - 1
    )));

  @TestWith({"testMetaDataPristine"})
  public void testEnergy(TestMetaData testMetaData) throws Exception {
    EnergyDriver driver = new EnergyDriver(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(DATASET_DIR_INPUT).toString(), dfsServer.getPath(DATASET_DIR_OUTPUT).toString()));
    assertCounterGreaterThan(testMetaData, driver.getCounters());
  }

  private static final String DATASET_DIR_INPUT = "/data";
  private static final String DATASET_DIR_OUTPUT = "/data/tmp";

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
