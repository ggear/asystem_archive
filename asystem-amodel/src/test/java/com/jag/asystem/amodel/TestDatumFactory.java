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
  public void testDatum() throws Exception {
    testDatum("default", DatumFactory.getDatum(),
      "getDataUnit", DatumDataUnit.__, "getDataMetric", DatumMetric.__);
    testDatum("indexed-1", DatumFactory.getDatumIndexed(1),
      "getDataUnit", DatumDataUnit.s, "getDataMetric", DatumMetric.anode__fronius__metrics);
    testDatum("indexed-50", DatumFactory.getDatumIndexed(50),
      "getDataUnit", DatumDataUnit.d, "getDataMetric", DatumMetric.energy__consumption_Dpeak_Dmorning__grid);
    testDatum("indexed-2147483647", DatumFactory.getDatumIndexed(21474836),
      "getDataUnit", DatumDataUnit.ppm, "getDataMetric", DatumMetric.energy__production_Dforecast_Da__inverter);
    testDatum("random", DatumFactory.getDatumRandom());
  }

  private void testDatum(String type, SpecificRecordBase record, Object... tests) throws Exception {
    if (tests.length % 2 == 0) {
      for (int i = 0; i < tests.length; i += 2) {
        Assert.assertEquals(tests[i + 1], record.getClass().getMethod((String) tests[i]).invoke(record));
      }
      LOG.info("Record of class [" + record.getClass().getSimpleName() + "] and instance type [" + type + "] serialized as [" +
        DATUM_FACTORY.serialize(record).length + "] bytes");
    }
  }

}