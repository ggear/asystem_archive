package com.jag.asystem.amodel.test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.cloudera.framework.testing.TestConstants;
import com.jag.asystem.amodel.DatumFactory;
import com.jag.asystem.amodel.avro.Datum;
import com.jag.asystem.amodel.avro.DatumDataUnit;
import com.jag.asystem.amodel.avro.DatumMetric;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatumFactoryTest implements TestConstants {

  private static final Logger LOG = LoggerFactory.getLogger(DatumFactoryTest.class);

  private static final DatumFactory DATUM_FACTORY = new DatumFactory();

  @Test
  public void testModelProperties() {
    assertNotNull(DatumFactory.getModelProperty("MODEL_VERSION"));
  }

  @Test
  public void testDecode() {
    assertEquals("",
      DatumFactory.decode(0, "1000", 1000));
    assertEquals("",
      DatumFactory.decode(0, "1000", 1000));
    assertEquals("1000",
      DatumFactory.decode(1, "1000", 1000));
    assertEquals("1000",
      DatumFactory.decode(1, "1000", 1000));
    assertEquals("1066",
      DatumFactory.decode(67, "1000", 1000));
    assertEquals("1066",
      DatumFactory.decode(67, "1000", 1000));
    assertEquals("1000-SNAPSHOT",
      DatumFactory.decode(-1, "1000-SNAPSHOT", 1000));
    assertEquals("1000-SNAPSHOT",
      DatumFactory.decode(-1, "1000-SNAPSHOT", 1000));
    assertEquals("6899-SNAPSHOT",
      DatumFactory.decode(-5900, "1000-SNAPSHOT", 1000));
    assertEquals("6899-SNAPSHOT",
      DatumFactory.decode(-5900, "1000-SNAPSHOT", 1000));
    assertEquals("",
      DatumFactory.decode(0, "10.000.0000", 100000000, 2, 6));
    assertEquals("",
      DatumFactory.decode(0, "10.000.0000", 100000000, 2, 6));
    assertEquals("10.000.0000",
      DatumFactory.decode(1, "10.000.0000", 100000000, 2, 6));
    assertEquals("10.000.0000",
      DatumFactory.decode(1, "10.000.0000", 100000000, 2, 6));
    assertEquals("10.000.0098",
      DatumFactory.decode(99, "10.000.0000", 100000000, 2, 6));
    assertEquals("10.000.0098",
      DatumFactory.decode(99, "10.000.0000", 100000000, 2, 6));
    assertEquals("10.156.5119",
      DatumFactory.decode(1565120, "10.000.0000", 100000000, 2, 6));
    assertEquals("10.156.5119",
      DatumFactory.decode(1565120, "10.000.0000", 100000000, 2, 6));
    assertEquals("15.651.20",
      DatumFactory.decode(1565120, "1.5.6-cdh5.12.0", 100000000, 2, 6));
    assertEquals("15.651.20",
      DatumFactory.decode(1565120, "1.5.6-cdh5.12.0", 100000000, 2, 6));
    assertEquals("10.000.0000-SNAPSHOT",
      DatumFactory.decode(-1, "10.000.0000-SNAPSHOT", 100000000, 2, 6));
    assertEquals("10.000.0000-SNAPSHOT",
      DatumFactory.decode(-1, "10.000.0000-SNAPSHOT", 100000000, 2, 6));
    assertEquals("10.156.5119-SNAPSHOT",
      DatumFactory.decode(-1565120, "10.000.0000-SNAPSHOT", 100000000, 2, 6));
    assertEquals("10.156.5119-SNAPSHOT",
      DatumFactory.decode(-1565120, "10.000.0000-SNAPSHOT", 100000000, 2, 6));
    assertEquals("15.651.20-SNAPSHOT",
      DatumFactory.decode(-1565120, "1.5.6-cdh5.12.0-SNAPSHOT", 100000000, 2, 6));
    assertEquals("15.651.20-SNAPSHOT",
      DatumFactory.decode(-1565120, "1.5.6-cdh5.12.0-SNAPSHOT", 100000000, 2, 6));
    assertEquals("38.999.9999-SNAPSHOT",
      DatumFactory.decode(-290000000, "10.000.0000-SNAPSHOT", 100000000, 2, 6));
    assertEquals("38.999.9999-SNAPSHOT",
      DatumFactory.decode(-290000000, "10.000.0000-SNAPSHOT", 100000000, 2, 6));
    assertEquals("°C",
      DatumFactory.decode(DatumDataUnit._PC2_PB0C));
    assertEquals("°C",
      DatumFactory.decode(DatumDataUnit._PC2_PB0C));
    assertEquals("km/h",
      DatumFactory.decode(DatumDataUnit.km_P2Fh));
    assertEquals("km/h",
      DatumFactory.decode(DatumDataUnit.km_P2Fh));
    assertEquals("ping.internet.new-york-city",
      DatumFactory.decode(DatumMetric.ping__internet__new_Dyork_Dcity));
    assertEquals("ping.internet.new-york-city",
      DatumFactory.decode(DatumMetric.ping__internet__new_Dyork_Dcity));
  }

  @Test
  public void testDatum() throws Exception {
    testDatum("default", DatumFactory.getDatum(),
      "getDataUnit", DatumDataUnit.__, "getDataMetric", DatumMetric.__);
    testDatum("indexed-1", DatumFactory.getDatumIndexed(1),
      "getDataUnit", DatumDataUnit.s, "getDataMetric", DatumMetric.anode__fronius__metrics);
    testDatum("indexed-50", DatumFactory.getDatumIndexed(50),
      "getDataUnit", DatumDataUnit._PC2_PB0, "getDataMetric", DatumMetric.energy__export__yield);
    testDatum("indexed-2147483647", DatumFactory.getDatumIndexed(21474836),
      "getDataUnit", DatumDataUnit.ms, "getDataMetric", DatumMetric.energy__production_Dforecast_D1023__inverter);
    testDatum("random", DatumFactory.getDatumRandom());
  }

  private void testDatum(String type, Datum datum, Object... tests) throws Exception {
    if (tests.length % 2 == 0) {
      for (int i = 0; i < tests.length; i += 2) {
        Assert.assertEquals(tests[i + 1], datum.getClass().getMethod((String) tests[i]).invoke(datum));
      }
      Assert.assertEquals(datum, DATUM_FACTORY.deserialize(DATUM_FACTORY.serialize(
        DATUM_FACTORY.deserialize(DATUM_FACTORY.serialize(datum), Datum.getClassSchema())), Datum.getClassSchema()));
      LOG.info("Record of class [" + datum.getClass().getSimpleName() + "] and instance type [" + type + "] serialized as [" +
        DATUM_FACTORY.serialize(datum).length + "] bytes");
    }
  }

}