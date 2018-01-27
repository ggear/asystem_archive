package com.jag.asystem.astore.test;

import static com.cloudera.framework.common.Driver.SUCCESS;
import static com.cloudera.framework.testing.Assert.assertCounterEquals;
import static com.jag.asystem.arouter.Constants.HDFS_DIR;
import static com.jag.asystem.astore.Counter.FILES_STAGED_DONE;
import static com.jag.asystem.astore.Counter.FILES_STAGED_SKIP;
import static org.junit.Assert.assertEquals;

import com.cloudera.framework.common.Driver;
import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestMetaData;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.SparkServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import com.jag.asystem.astore.Rewrite;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(TestRunner.class)
public class RewriteTest implements TestConstants {

  @ClassRule
  public static final DfsServer dfsServer = DfsServer.getInstance();

  @ClassRule
  public static final SparkServer sparkServer = SparkServer.getInstance();

  public final TestMetaData testRewrite = TestMetaData.getInstance().dataSetSourceDirs(REL_DIR_DATASET)
    .dataSetNames("astore").dataSetSubsets(new String[][]{{"datums"}})
    .dataSetLabels(new String[][][]{{{"rewrite"}}})
    .dataSetDestinationDirs(HDFS_DIR)
    .asserts(ImmutableMap.of(Rewrite.class.getName(), ImmutableMap.builder()
      .put(FILES_STAGED_SKIP, 0L)
      .put(FILES_STAGED_DONE, 19L)
      .build())
    );

  private static final Logger LOG = LoggerFactory.getLogger(RewriteTest.class);

  @TestWith({"testRewrite"})
  public void testRewrite(TestMetaData test) {
    Driver driver = new Rewrite(dfsServer.getConf());
    assertEquals(SUCCESS, driver.runner(dfsServer.getPath(HDFS_DIR).toString()));
    assertCounterEquals(test, driver.getCounters());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

}
