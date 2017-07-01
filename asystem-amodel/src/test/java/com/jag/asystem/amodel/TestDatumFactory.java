package com.jag.asystem.amodel;

import com.cloudera.framework.testing.TestConstants;
import com.jag.asystem.amodel.avro.DatumDataUnit;
import com.jag.asystem.amodel.avro.DatumMetric;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDatumFactory implements TestConstants {

  private static final Logger LOG = LoggerFactory.getLogger(TestDatumFactory.class);

  private static final DatumFactory DATUM_FACTORY = new DatumFactory();

  @Test
  public void testDatumMetadataUnion() throws Exception {
    testDatum("indexed-1", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(1)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.fronius.metrics", "getDataUnit", "s");
    testDatum("indexed-13", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(13)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.davis.up-time", "getDataUnit", "mbar");
    testDatum("indexed-14", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(14)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.davis.last-seen", "getDataUnit", "$");
    testDatum("indexed-15", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(15)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.netatmo.metrics", "getDataUnit", "%");
    testDatum("indexed-16", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(16)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.netatmo.queue", "getDataUnit", "m/s");
    testDatum("indexed-17", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(17)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.netatmo.buffer", "getDataUnit", "km/h");
    testDatum("indexed-18", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(18)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.netatmo.history", "getDataUnit", "mm/h");
    testDatum("indexed-19", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(19)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.netatmo.partitions", "getDataUnit", "°");
    testDatum("indexed-20", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(20)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "anode.netatmo.up-time", "getDataUnit", "°C");
    testDatum("indexed-50", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(50)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "energy.consumption-off-peak-morning.grid", "getDataUnit", "d");
    testDatum("indexed-2147483647", DatumFactory.getDatumMetadataUnion(DatumFactory.getDatumMetadata("one", 1L), DatumFactory.getDatumIndexed(2147483647)),
      "getIngestId", "one", "getIngestTimestamp", 1L, "getDataMetric", "humidity.indoor.dining", "getDataUnit", "W");
  }

  @Test
  public void testDatumMetadata() throws Exception {
    testDatum("default", DatumFactory.getDatumMetadata());
    testDatum("default", DatumFactory.getDatumMetadata("one", 1L),
      "getIngestId", "one", "getIngestTimestamp", 1L);
  }

  @Test
  public void testDatum() throws Exception {
    testDatum("default", DatumFactory.getDatum(),
      "getDataUnit", DatumDataUnit.__, "getDataMetric", DatumMetric.__);
    testDatum("indexed-1", DatumFactory.getDatumIndexed(1),
      "getDataUnit", DatumDataUnit.s, "getDataMetric", DatumMetric.anode__fronius__metrics);
    testDatum("indexed-50", DatumFactory.getDatumIndexed(50),
      "getDataUnit", DatumDataUnit.d, "getDataMetric", DatumMetric.energy__consumption_Doff_Dpeak_Dmorning__grid);
    testDatum("indexed-2147483647", DatumFactory.getDatumIndexed(2147483647),
      "getDataUnit", DatumDataUnit.W, "getDataMetric", DatumMetric.humidity__indoor__dining);
    testDatum("random", DatumFactory.getDatumRandom());
  }

  private void testDatum(String type, SpecificRecordBase record, Object... tests) throws Exception {
    if (tests.length % 2 == 0) {
      for (int i = 0; i < tests.length; i += 2) {
        Assert.assertEquals(tests[i + 1], record.getClass().getMethod((String) tests[i]).invoke(record));
      }
      LOG.info("Record of class [" + record.getClass().getSimpleName() + "] and instance type [" + type + "] serialized as [" + DATUM_FACTORY.serialize(record).length + "] bytes");
    }
  }

}