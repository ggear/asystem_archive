package com.jag.asystem.amodel;

import java.io.ByteArrayOutputStream;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.cloudera.framework.common.Driver;
import com.google.common.collect.ImmutableMap.Builder;
import com.jag.asystem.amodel.avro.Datum;
import com.jag.asystem.amodel.avro.DatumBinUnit;
import com.jag.asystem.amodel.avro.DatumDataUnit;
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

  private transient ThreadLocal<BinaryEncoder> datumEncoder;

  private transient ThreadLocal<Map<String, DatumWriter<SpecificRecordBase>>> datumWriters;

  public static Datum getDatum() {
    return Datum.newBuilder().build();
  }

  public static Datum getDatumRandom() {
    return Datum.newBuilder()
      .setAsystemVersion(Integer.parseInt(Driver.getApplicationProperty("APP_VERSION_NUMERIC")))
      .setDataVersion(0)
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
      .setAsystemVersion(Integer.parseInt(Driver.getApplicationProperty("APP_VERSION_NUMERIC")) + index)
      .setDataVersion(0)
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

  protected static String decode(int encoded, String raw, int base, int... dividers) {
    String key = new StringBuilder().append(encoded).append("_").append(raw).append("_").
      append(base).append("_").append(Arrays.toString(dividers)).toString();
    String decoded = ENCODING_CACHE.get(key);
    if (decoded == null) {
      try {
        if (encoded == 0) {
          decoded = "";
        } else {
          StringBuilder decodedBuilder = new StringBuilder("" +
            (Math.abs(encoded) + (("" + base).length() == raw.replaceAll("[\\D]", "").length() ? (base - 1) : 0)));
          for (int divider : dividers) {
            decodedBuilder.insert(divider, ".");
          }
          decoded = decodedBuilder.append(encoded > 0 ? "" : "-SNAPSHOT").toString();
        }
        ENCODING_CACHE.put(key, decoded);
      } catch (Exception e) {
        throw new RuntimeException("Could not decode [" + encoded + "]", e);
      }
    }
    return decoded;
  }

  protected static String decode(Enum encoded) {
    String decoded = ENCODING_CACHE.get(encoded);
    if (decoded == null) {
      try {
        String[] decodes = encoded.toString().split("__");
        IntStream.range(0, decodes.length).forEach(i ->
          ESCAPE_SEQUENCES.forEach((escaped, unescaped) ->
            ESCAPE_SWAPS.forEach((swap, swapped) ->
              decodes[i] = Arrays.stream(decodes[i].replace(escaped, unescaped).split(Pattern.quote(swap), -1)).map(s ->
                s.replaceAll(Pattern.quote(swapped), swap)).collect(Collectors.joining(swapped)))));
        decoded = URLDecoder.decode(String.join(".", Arrays.asList(decodes)), "UTF-8");
      } catch (Exception e) {
        throw new RuntimeException("Could not decode [" + encoded + "]", e);
      }
    }
    return decoded;
  }

  private static <T extends Enum<?>> T getEnumRandom(Class<T> clazz) {
    return clazz.getEnumConstants()[ThreadLocalRandom.current().nextInt(clazz.getEnumConstants().length - 1) + 1];
  }

  private static <T extends Enum<?>> T getEnumIndexed(Class<T> clazz, int index) {
    return clazz.getEnumConstants()[index % clazz.getEnumConstants().length];
  }

  private DatumWriter<SpecificRecordBase> getDatumWriter(Schema schema) {
    if (datumWriters == null) {
      datumWriters = ThreadLocal.withInitial(HashMap::new);
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

  private static Map<Object, String> ENCODING_CACHE = Collections.synchronizedMap(new LinkedHashMap<Object, String>() {
    @Override
    protected boolean removeEldestEntry(Map.Entry<Object, String> eldest) {
      return size() > 100000;
    }
  });

}
