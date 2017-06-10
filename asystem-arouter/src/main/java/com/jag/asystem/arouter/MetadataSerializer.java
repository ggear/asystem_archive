package com.jag.asystem.arouter;

import java.util.Collections;

import com.jag.asystem.amodel.DatumFactory;
import com.jag.asystem.amodel.avro.DatumMetaData;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.hdfs.SequenceFileSerializer;
import org.apache.hadoop.io.BytesWritable;

@SuppressWarnings({"WeakerAccess", "unused"})
public class MetadataSerializer implements SequenceFileSerializer {

  @Override
  public Class<DatumMetaData> getKeyClass() {
    return DatumMetaData.class;
  }

  @Override
  public Class<BytesWritable> getValueClass() {
    return BytesWritable.class;
  }

  @Override
  public Iterable<Record> serialize(Event event) {
    return Collections.singletonList(new Record(
      DatumFactory.getDatumMetadataDefault(event.getHeaders().get(MetadataInterceptor.HEADER_INGEST_ID), System.currentTimeMillis()),
      new BytesWritable(event.getBody())));
  }

  @SuppressWarnings("unused")
  public static class Builder implements SequenceFileSerializer.Builder {

    @Override
    public SequenceFileSerializer build(Context context) {
      return new MetadataSerializer();
    }

  }

}
