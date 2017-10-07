package com.jag.asystem.arouter;

import static org.apache.flume.sink.hdfs.AvroEventSerializer.AVRO_SCHEMA_URL_HEADER;

import java.util.List;
import java.util.UUID;

import javax.xml.bind.DatatypeConverter;

import com.cloudera.framework.common.flume.MqttSource;
import com.google.common.base.Joiner;
import com.jag.asystem.amodel.DatumFactory;
import com.jag.asystem.amodel.avro.Datum;
import com.jag.asystem.amodel.avro.Datum;
import com.jag.asystem.amodel.avro.DatumAnodeId;
import com.jag.asystem.amodel.avro.DatumAnodeId;
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
  public static final String HEADER_ANODE_ID = "aid";
  public static final String HEADER_AMODEL_VERSION = "amv";
  public static final String HEADER_ASYSTEM_VERSION = "asv";

  private static final Logger LOG = LoggerFactory.getLogger(MetadataInterceptor.class);

  private static final DatumFactory DATUM_FACTORY = new DatumFactory();

  private static final String AVRO_SCHEMA_FILE = "datum.avsc";
  private static final String INSTANCE_ID = "flume-" + UUID.randomUUID().toString();

  private final int batchSize;
  private final String avroSchemaUrl;
  private final boolean dropSnapshots;

  private int batchCount;
  private long batchTimestamp;

  private MetadataInterceptor(int batchSize, String avroSchemaUrl, boolean dropSnapshots) {
    this.batchSize = batchSize;
    this.avroSchemaUrl = avroSchemaUrl;
    this.dropSnapshots = dropSnapshots;
  }

  @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
  private static String putHeader(Event event, String key, String value, boolean force) {
    String valuePrevious = event.getHeaders().get(key);
    if (force || valuePrevious == null) {
      event.getHeaders().put(key, value);
      if (LOG.isDebugEnabled()) {
        LOG.debug("MQTT adding event header [" + key + "] with value [" + value + "]"
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
    String[] topicSegments;
    String topic = event.getHeaders().get(MqttSource.HEADER_TOPIC);
    if (topic == null || (topicSegments = topic.split("/")).length != 6) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("MQTT event missing valid header attributes [" +
          Joiner.on(",").withKeyValueSeparator("=").join(event.getHeaders()) + "]");
      }
      return null;
    }
    if (dropSnapshots && topicSegments[1].endsWith("SNAPSHOT")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("MQTT event being dropped since it has a snapshot version [" + topicSegments[1] + "]");
      }
      return null;
    }
    if (batchCount == 0 || batchCount >= batchSize) {
      batchTimestamp = System.currentTimeMillis();
    }
    if (++batchCount >= batchSize) {
      batchCount = 0;
    }
    int size = event.getBody().length;
    Datum datum = DATUM_FACTORY.deserialize(event.getBody(), Datum.getClassSchema());
    datum.setAnodeId(new DatumAnodeId(DatatypeConverter.parseHexBinary(topicSegments[3])));
    event.setBody(DATUM_FACTORY.serialize(datum));
    if (LOG.isDebugEnabled()) {
      LOG.debug("MQTT added event meta-data to datum with size increase of [" + (event.getBody().length - size) + "]");
    }
    putHeader(event, HEADER_INGEST_ID, INSTANCE_ID, false);
    putHeader(event, HEADER_INGEST_PARTITION, "" + batchTimestamp % 10, false);
    putHeader(event, HEADER_INGEST_TIMESTAMP, "" + batchTimestamp, false);
    putHeader(event, AVRO_SCHEMA_URL_HEADER, avroSchemaUrl + "/" + topicSegments[5] + "/" + AVRO_SCHEMA_FILE, false);
    putHeader(event, HEADER_ASYSTEM_VERSION, topicSegments[1], false);
    putHeader(event, HEADER_ANODE_ID, topicSegments[3], false);
    putHeader(event, HEADER_AMODEL_VERSION, topicSegments[5], false);
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
    public static final String CONFIG_DROP_SNAPSHOTS = "dropSnapshots";

    private int batchSize;
    private String avroSchemaUrl;
    private boolean dropSnapshots;

    public Builder() {
    }

    @Override
    public MetadataInterceptor build() {
      return new MetadataInterceptor(batchSize, avroSchemaUrl, dropSnapshots);
    }

    @Override
    public void configure(Context context) {
      batchSize = context.getInteger(CONFIG_BATCH_SIZE, 1);
      avroSchemaUrl = context.getString(CONFIG_AVRO_SCHEMA_URL, "").trim();
      dropSnapshots = context.getBoolean(CONFIG_DROP_SNAPSHOTS, false);
    }

  }

}
