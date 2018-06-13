package com.jag.asystem.amodel;

import com.cloudera.framework.common.navigator.MetaDataExecution;
import com.cloudera.framework.common.navigator.MetaDataTemplate;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;

@MClass(model = "asystem_amodel_energyforecast_execution")
public class Execution extends MetaDataExecution {

  @MProperty(attribute = "TESTING_INSTANCES")
  private String testingInstances;

  @MProperty(attribute = "TRAINING_INSTANCES")
  private String trainingInstances;

  @MProperty(attribute = "VALIDATION_INSTANCES")
  private String validationInstances;

  @MProperty(attribute = "VALIDATION_RMS_ERROR")
  private String validationRmsError;

  @MProperty(attribute = "VALIDATION_MEAN_ACCURACY")
  private String validationMeanAccuracy;

  public Execution() {
  }

  public Execution(Configuration conf, MetaDataTemplate template, Integer exit, Instant started, Instant ended) {
    super(conf, template, exit);
    setStarted(started);
    setEnded(ended);
  }

  public String getTestingInstances() {
    return testingInstances;
  }

  public void setTestingInstances(String testingInstances) {
    this.testingInstances = testingInstances;
  }

  public String getTrainingInstances() {
    return trainingInstances;
  }

  public void setTrainingInstances(String trainingInstances) {
    this.trainingInstances = trainingInstances;
  }

  public String getValidationInstances() {
    return validationInstances;
  }

  public void setValidationInstances(String validationInstances) {
    this.validationInstances = validationInstances;
  }

  public String getValidationRmsError() {
    return validationRmsError;
  }

  public void setValidationRmsError(String validationRmsError) {
    this.validationRmsError = validationRmsError;
  }

  public String getValidationMeanAccuracy() {
    return validationMeanAccuracy;
  }

  public void setValidationMeanAccuracy(String validationMeanAccuracy) {
    this.validationMeanAccuracy = validationMeanAccuracy;
  }

}
