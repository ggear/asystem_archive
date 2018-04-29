package com.jag.asystem.astore;

import com.cloudera.framework.common.navigator.MetaDataTemplate;
import com.cloudera.nav.sdk.model.annotations.MClass;
import org.apache.hadoop.conf.Configuration;

@MClass(model = "asystem_astore_process_template")
public class Template extends MetaDataTemplate {

  public Template(Configuration conf) {
    super(conf);
  }

}
