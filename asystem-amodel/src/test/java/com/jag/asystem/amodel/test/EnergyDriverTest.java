package com.jag.asystem.amodel.test;

import com.jag.asystem.amodel.EnergyDriver;

import static com.cloudera.framework.common.Driver.Counter.FILES_OUT;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_IN;
import static com.cloudera.framework.common.Driver.Counter.RECORDS_OUT;
import static com.cloudera.framework.common.Driver.FAILURE_ARGUMENTS;
import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
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
import org.junit.Test;
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
      FILES_OUT, 3,
      RECORDS_IN, 100L,
      RECORDS_OUT, 100L
    )));

  @TestWith({"testMetaDataPristine"})
  public void testEnergy(TestMetaData testMetaData) throws Exception {
    EnergyDriver driver = new EnergyDriver(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(DATASET_DIR_INPUT).toString(), dfsServer.getPath(DATASET_DIR_OUTPUT).toString()));

    // TODO
    //    assertCounterEquals(testMetaData, driver.getCounters());
    //    assertCounterEquals(testMetaData, Driver.class.getName(), FILES_OUT, dfsServer.listFilesDfs(DATASET_DIR_OUTPUT).length);

  }

  private static final String DATASET_DIR_INPUT = "/data";
  private static final String DATASET_DIR_OUTPUT = "/data/tmp";

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
