package com.jag.asystem.arouter;

import com.cloudera.framework.testing.server.DfsServer;
import com.cloudera.framework.testing.server.FlumeServer;
import com.cloudera.framework.testing.server.MqttServer;
import com.jag.asystem.arouter.test.Flume;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ //
  Flume.class
})
public class TestSuite {

  @ClassRule
  public static TestRule cdhServers = RuleChain //
    .outerRule(DfsServer.getInstance(DfsServer.Runtime.CLUSTER_DFS)) //
    .around(MqttServer.getInstance(MqttServer.Runtime.LOCAL_BROKER)) //
    .around(FlumeServer.getInstance(FlumeServer.Runtime.MANUALLY_CRANKED));

}
