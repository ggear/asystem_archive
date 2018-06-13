package com.jag.asystem.amodel;

import com.cloudera.framework.common.navigator.MetaDataTemplate;
import com.cloudera.nav.sdk.model.annotations.MClass;
import org.apache.hadoop.conf.Configuration;

@MClass(model = "asystem_amodel_energyforecast_template")
public class Template extends MetaDataTemplate {

  public Template(Configuration conf) {
    super(conf);
  }

}
