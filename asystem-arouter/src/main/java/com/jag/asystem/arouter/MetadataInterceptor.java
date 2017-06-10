package com.jag.asystem.arouter;

import static org.apache.flume.sink.hdfs.AvroEventSerializer.AVRO_SCHEMA_URL_HEADER;

import java.util.List;
import java.util.UUID;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add meata-data to Flume header of each event
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class MetadataInterceptor implements Interceptor {

  public static final String HEADER_INGEST_ID = "iid";
  public static final String HEADER_INGEST_PARTITION = "ipt";
  public static final String HEADER_INGEST_TIMESTAMP = "its";

  private static final Logger LOG = LoggerFactory.getLogger(MetadataInterceptor.class);

  private static final String INSTANCE_ID = "flume-" + UUID.randomUUID().toString();

  private final int batchSize;
  private final String avroSchemaUrl;
  private int batchCount;
  private long batchTimestamp;

  private MetadataInterceptor(int batchSize, String avroSchemaUrl) {
    this.batchSize = batchSize;
    this.avroSchemaUrl = avroSchemaUrl;
  }

  @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
  private static String putHeader(Event event, String key, String value, boolean force) {
    String valuePrevious = event.getHeaders().get(key);
    if (force || valuePrevious == null) {
      event.getHeaders().put(key, value);
      if (LOG.isTraceEnabled()) {
        LOG.trace("MQTT adding event header [" + key + "] with value [" + value + "]"
          + (valuePrevious == null ? "" : " overwriting previous value [" + valuePrevious + "]"));
      }
    }
    return force || valuePrevious == null ? value : valuePrevious;
  }

  @Override
  public void initialize() {
  }

  @Override
  public Event intercept(Event event) {
    if (batchCount == 0 || batchCount >= batchSize) {
      batchTimestamp = System.currentTimeMillis();
    }
    if (++batchCount >= batchSize) {
      batchCount = 0;
    }
    putHeader(event, HEADER_INGEST_ID, INSTANCE_ID, false);
    putHeader(event, HEADER_INGEST_PARTITION, "" + batchTimestamp % 10, false);
    putHeader(event, HEADER_INGEST_TIMESTAMP, "" + batchTimestamp, false);
    putHeader(event, AVRO_SCHEMA_URL_HEADER, avroSchemaUrl, false);
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    events.forEach(this::intercept);
    return events;
  }

  @Override
  public void close() {
  }

  @SuppressWarnings("unused")
  public static class Builder implements Interceptor.Builder {

    public static final String CONFIG_BATCH_SIZE = "batchSize";
    public static final String CONFIG_AVRO_SCHEMA_URL = "avroSchemaURL";

    private int batchSize;
    private String avroSchemaUrl;

    public Builder() {
    }

    @Override
    public MetadataInterceptor build() {
      return new MetadataInterceptor(batchSize, avroSchemaUrl);
    }

    @Override
    public void configure(Context context) {
      batchSize = context.getInteger(CONFIG_BATCH_SIZE, 1);
      avroSchemaUrl = context.getString(CONFIG_AVRO_SCHEMA_URL, "").trim();
    }

  }

}
