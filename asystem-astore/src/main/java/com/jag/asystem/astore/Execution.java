package com.jag.asystem.astore;

import com.cloudera.framework.common.navigator.MetaDataExecution;
import com.cloudera.framework.common.navigator.MetaDataTemplate;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;

@MClass(model = "asystem_astore_process_execution")
public class Execution extends MetaDataExecution {

  @MProperty(attribute = "STAGED_FILES_FAIL")
  private String stagedFilesFail;

  @MProperty(attribute = "STAGED_FILES_TEMP")
  private String stagedFilesTemp;

  @MProperty(attribute = "STAGED_FILES_PURE")
  private String stagedFilesPure;

  @MProperty(attribute = "STAGED_PARTITIONS_TEMP")
  private String stagedPartitionsTemp;

  @MProperty(attribute = "STAGED_PARTITIONS_SKIP")
  private String stagedPartitionsSkip;

  @MProperty(attribute = "STAGED_PARTITIONS_REDO")
  private String stagedPartitionsRedo;

  @MProperty(attribute = "STAGED_PARTITIONS_DONE")
  private String stagedPartitionsDone;

  @MProperty(attribute = "PROCESSED_FILES_FAIL")
  private String processedFilesFail;

  @MProperty(attribute = "PROCESSED_FILES_PURE")
  private String processedFilesPure;

  @MProperty(attribute = "PROCESSED_PARTITIONS_SKIP")
  private String processedPartitionsSkip;

  @MProperty(attribute = "PROCESSED_PARTITIONS_REDO")
  private String processedPartitionsRedo;

  @MProperty(attribute = "PROCESSED_PARTITIONS_DONE")
  private String processedPartitionsDone;

  public Execution() {
  }

  public Execution(Configuration conf, MetaDataTemplate template, Integer exit, Instant started, Instant ended) {
    super(conf, template, exit);
    setStarted(started);
    setEnded(ended);
  }

  public String getStagedFilesFail() {
    return stagedFilesFail;
  }

  public void setStagedFilesFail(String stagedFilesFail) {
    this.stagedFilesFail = stagedFilesFail;
  }

  public String getStagedFilesTemp() {
    return stagedFilesTemp;
  }

  public void setStagedFilesTemp(String stagedFilesTemp) {
    this.stagedFilesTemp = stagedFilesTemp;
  }

  public String getStagedFilesPure() {
    return stagedFilesPure;
  }

  public void setStagedFilesPure(String stagedFilesPure) {
    this.stagedFilesPure = stagedFilesPure;
  }

  public String getStagedPartitionsTemp() {
    return stagedPartitionsTemp;
  }

  public void setStagedPartitionsTemp(String stagedPartitionsTemp) {
    this.stagedPartitionsTemp = stagedPartitionsTemp;
  }

  public String getStagedPartitionsSkip() {
    return stagedPartitionsSkip;
  }

  public void setStagedPartitionsSkip(String stagedPartitionsSkip) {
    this.stagedPartitionsSkip = stagedPartitionsSkip;
  }

  public String getStagedPartitionsRedo() {
    return stagedPartitionsRedo;
  }

  public void setStagedPartitionsRedo(String stagedPartitionsRedo) {
    this.stagedPartitionsRedo = stagedPartitionsRedo;
  }

  public String getStagedPartitionsDone() {
    return stagedPartitionsDone;
  }

  public void setStagedPartitionsDone(String stagedPartitionsDone) {
    this.stagedPartitionsDone = stagedPartitionsDone;
  }

  public String getProcessedFilesFail() {
    return processedFilesFail;
  }

  public void setProcessedFilesFail(String processedFilesFail) {
    this.processedFilesFail = processedFilesFail;
  }

  public String getProcessedFilesPure() {
    return processedFilesPure;
  }

  public void setProcessedFilesPure(String processedFilesPure) {
    this.processedFilesPure = processedFilesPure;
  }

  public String getProcessedPartitionsSkip() {
    return processedPartitionsSkip;
  }

  public void setProcessedPartitionsSkip(String processedPartitionsSkip) {
    this.processedPartitionsSkip = processedPartitionsSkip;
  }

  public String getProcessedPartitionsRedo() {
    return processedPartitionsRedo;
  }

  public void setProcessedPartitionsRedo(String processedPartitionsRedo) {
    this.processedPartitionsRedo = processedPartitionsRedo;
  }

  public String getProcessedPartitionsDone() {
    return processedPartitionsDone;
  }

  public void setProcessedPartitionsDone(String processedPartitionsDone) {
    this.processedPartitionsDone = processedPartitionsDone;
  }

}
