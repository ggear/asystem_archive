package com.jag.asystem.astore;

public enum Counter {

  STAGED_ROWS_PURE,

  STAGED_FILES_FAIL,
  STAGED_FILES_TEMP,
  STAGED_FILES_PURE,

  STAGED_PARTITIONS_TEMP,
  STAGED_PARTITIONS_SKIP,
  STAGED_PARTITIONS_TODO,
  STAGED_PARTITIONS_REDO,
  STAGED_PARTITIONS_DONE,

  PROCESSED_ROWS_PURE,

  PROCESSED_FILES_FAIL,
  PROCESSED_FILES_PURE,

  PROCESSED_PARTITIONS_SKIP,
  PROCESSED_PARTITIONS_EXEC,
  PROCESSED_PARTITIONS_REDO,
  PROCESSED_PARTITIONS_DONE,

}