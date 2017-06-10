package com.jag.asystem.amodel;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.jag.asystem.amodel.avro.Datum;
import com.jag.asystem.amodel.avro.DatumAnodeId;
import com.jag.asystem.amodel.avro.DatumBinUnit;
import com.jag.asystem.amodel.avro.DatumDataUnit;
import com.jag.asystem.amodel.avro.DatumMetaData;
import com.jag.asystem.amodel.avro.DatumMetric;
import com.jag.asystem.amodel.avro.DatumSource;
import com.jag.asystem.amodel.avro.DatumTemporal;
import com.jag.asystem.amodel.avro.DatumType;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

@SuppressWarnings("unused")
public class DatumFactory {

  private static final DatumWriter<Datum> DATUM_WRITER = new GenericDatumWriter<>(Datum.getClassSchema());

  public static DatumMetaData getDatumMetadataDefault(String ingestId, Long ingestTimestamp) {
    return DatumMetaData.newBuilder()
      .setIngestId(ingestId == null ? UUID.randomUUID().toString() : ingestId)
      .setIngestTimestamp(ingestTimestamp == null ? System.currentTimeMillis() : ingestTimestamp)
      .build();
  }

  public static Datum getDatumDefault() {
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

  public static byte[] serializeDatum(Datum datum) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(40);
    try {
      BinaryEncoder datumEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      DATUM_WRITER.write(datum, datumEncoder);
      datumEncoder.flush();
    } catch (Exception e) {
      throw new RuntimeException("Could not serialise datum", e);
    }
    return outputStream.toByteArray();
  }

  private static <T extends Enum<?>> T getEnumRandom(Class<T> clazz) {
    return clazz.getEnumConstants()[ThreadLocalRandom.current().nextInt(clazz.getEnumConstants().length - 1) + 1];
  }

  private static <T extends Enum<?>> T getEnumIndexed(Class<T> clazz, int index) {
    return clazz.getEnumConstants()[index % clazz.getEnumConstants().length];
  }

}
