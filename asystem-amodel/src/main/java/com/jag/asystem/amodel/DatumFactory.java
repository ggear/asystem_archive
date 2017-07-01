package com.jag.asystem.amodel;

import java.io.ByteArrayOutputStream;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap.Builder;
import com.jag.asystem.amodel.avro.Datum;
import com.jag.asystem.amodel.avro.DatumAnodeId;
import com.jag.asystem.amodel.avro.DatumBinUnit;
import com.jag.asystem.amodel.avro.DatumDataUnit;
import com.jag.asystem.amodel.avro.DatumMetaData;
import com.jag.asystem.amodel.avro.DatumMetadataUnion;
import com.jag.asystem.amodel.avro.DatumMetric;
import com.jag.asystem.amodel.avro.DatumSource;
import com.jag.asystem.amodel.avro.DatumTemporal;
import com.jag.asystem.amodel.avro.DatumType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;

@SuppressWarnings("unused")
public class DatumFactory {

  public static DatumMetadataUnion getDatumMetadataUnion() {
    return DatumMetadataUnion.newBuilder().build();
  }

  public static DatumMetadataUnion getDatumMetadataUnion(DatumMetaData metadata, Datum datum) {
    return DatumMetadataUnion.newBuilder()
      .setIngestId(metadata.getIngestId())
      .setIngestTimestamp(metadata.getIngestTimestamp())
      .setAnodeId(datum.getAnodeId())
      .setDataSource(decode(datum.getDataSource()))
      .setDataMetric(decode(datum.getDataMetric()))
      .setDataTemporal(decode(datum.getDataTemporal()))
      .setDataType(decode(datum.getDataType()))
      .setDataValue(datum.getDataValue())
      .setDataUnit(decode(datum.getDataUnit()))
      .setDataScale(datum.getDataScale())
      .setDataString(datum.getDataString())
      .setDataTimestamp(datum.getDataTimestamp())
      .setBinTimestamp(datum.getBinTimestamp())
      .setBinWidth(datum.getBinWidth())
      .setBinUnit(decode(datum.getDataUnit()))
      .build();
  }

  public static DatumMetaData getDatumMetadata() {
    return DatumMetaData.newBuilder().build();
  }

  public static DatumMetaData getDatumMetadata(String ingestId, Long ingestTimestamp) {
    return DatumMetaData.newBuilder()
      .setIngestId(ingestId == null ? UUID.randomUUID().toString() : ingestId)
      .setIngestTimestamp(ingestTimestamp == null ? System.currentTimeMillis() : ingestTimestamp)
      .build();
  }

  public static Datum getDatum() {
    return Datum.newBuilder().build();
  }

  public static Datum getDatumRandom() {
    byte[] anodeId = new byte[6];
    ThreadLocalRandom.current().nextBytes(anodeId);
    return Datum.newBuilder()
      .setAnodeId(new DatumAnodeId(anodeId))
      .setDataSource(getEnumRandom(DatumSource.class))
      .setDataMetric(getEnumRandom(DatumMetric.class))
      .setDataTemporal(getEnumRandom(DatumTemporal.class))
      .setDataType(getEnumRandom(DatumType.class))
      .setDataValue(ThreadLocalRandom.current().nextLong())
      .setDataUnit(getEnumRandom(DatumDataUnit.class))
      .setDataScale(ThreadLocalRandom.current().nextFloat())
      .setDataString(UUID.randomUUID().toString())
      .setDataTimestamp(System.currentTimeMillis() / 1000 - ThreadLocalRandom.current().nextInt(10))
      .setBinTimestamp(System.currentTimeMillis() / 1000)
      .setBinWidth(ThreadLocalRandom.current().nextInt())
      .setBinUnit(getEnumRandom(DatumBinUnit.class))
      .build();
  }

  @SuppressWarnings("SameParameterValue")
  public static Datum getDatumIndexed(int index) {
    return Datum.newBuilder()
      .setAnodeId(new DatumAnodeId(ByteBuffer.allocate(6).putInt(index).array()))
      .setDataSource(getEnumIndexed(DatumSource.class, index))
      .setDataMetric(getEnumIndexed(DatumMetric.class, index))
      .setDataTemporal(getEnumIndexed(DatumTemporal.class, index))
      .setDataType(getEnumIndexed(DatumType.class, index))
      .setDataValue((long) index)
      .setDataUnit(getEnumIndexed(DatumDataUnit.class, index))
      .setDataScale((float) index)
      .setDataString(null)
      .setDataTimestamp(System.currentTimeMillis() / 1000 - ThreadLocalRandom.current().nextInt(10))
      .setBinTimestamp(System.currentTimeMillis() / 1000)
      .setBinWidth(index)
      .setBinUnit(getEnumIndexed(DatumBinUnit.class, index))
      .build();
  }

  private DatumWriter<SpecificRecordBase> getDatumWriter(Schema schema) {
    if (datumWriters == null) {
      datumWriters = ThreadLocal.withInitial(() -> new HashMap<>());
    }
    if (!datumWriters.get().containsKey(schema.toString())) {
      datumWriters.get().put(schema.toString(), new GenericDatumWriter<>(schema));
    }
    return datumWriters.get().get(schema.toString());
  }

  private ThreadLocal<BinaryEncoder> getDatumEncoder() {
    if (datumEncoder == null) {
      datumEncoder = new ThreadLocal<>();
    }
    return datumEncoder;
  }

  public byte[] serialize(SpecificRecordBase record) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(40);
    try {
      ThreadLocal<BinaryEncoder> encoder = getDatumEncoder();
      BinaryEncoder encoderInstance = EncoderFactory.get().binaryEncoder(outputStream, encoder.get());
      encoder.set(encoderInstance);
      getDatumWriter(record.getSchema()).write(record, encoderInstance);
      encoderInstance.flush();
    } catch (Exception e) {
      throw new RuntimeException("Could not serialise record", e);
    }
    return outputStream.toByteArray();
  }

  private transient ThreadLocal<BinaryEncoder> datumEncoder;
  private transient ThreadLocal<Map<String, DatumWriter<SpecificRecordBase>>> datumWriters;

  private static String swap(String string, String target, String replacement) {
    return Arrays.stream(string.split(Pattern.quote(target), -1)).map(s ->
      s.replaceAll(Pattern.quote(replacement), target)).collect(Collectors.joining(replacement));
  }

  private static String decode(Enum field) {
    String decoded;
    try {
      String[] decodeds = field.toString().split("__");
      IntStream.range(0, decodeds.length).forEach(i ->
        ESCAPE_SEQUENCES.forEach((escaped, unescaped) ->
          ESCAPE_SWAPS.forEach((swap, swapped) ->
            decodeds[i] = swap(decodeds[i].replace(escaped, unescaped), swap, swapped))));
      decoded = URLDecoder.decode(String.join(".", Arrays.asList(decodeds)), "UTF-8");
    } catch (Exception e) {
      throw new RuntimeException("Could not decode [" + field + "]", e);
    }
    return decoded;
  }

  private static final Map<String, String> ESCAPE_SWAPS =
    new Builder<String, String>()
      .put("_", ".")
      .put(".", "_")
      .build();

  private static final Map<String, String> ESCAPE_SEQUENCES =
    new Builder<String, String>()
      .put("__", "_")
      .put("_X", ".")
      .put("_D", "-")
      .put("_P", "%")
      .build();

  private static <T extends Enum<?>> T getEnumRandom(Class<T> clazz) {
    return clazz.getEnumConstants()[ThreadLocalRandom.current().nextInt(clazz.getEnumConstants().length - 1) + 1];
  }

  private static <T extends Enum<?>> T getEnumIndexed(Class<T> clazz, int index) {
    return clazz.getEnumConstants()[index % clazz.getEnumConstants().length];
  }

}
