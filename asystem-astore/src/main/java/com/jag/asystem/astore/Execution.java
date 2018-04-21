package com.jag.asystem.astore;

import com.cloudera.framework.common.navigator.MetaDataExecution;
import com.cloudera.framework.common.navigator.MetaDataTemplate;
import com.cloudera.nav.sdk.model.annotations.MClass;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;

@MClass(model = "process_execution")
public class Execution extends MetaDataExecution {

  public Execution(Configuration conf, MetaDataTemplate template, String version, Instant started, Instant ended) {
    super(conf, template, version);
    setStarted(started);
    setEnded(ended);
  }

}
