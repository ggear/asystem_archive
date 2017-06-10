package com.jag.asystem.amodel;

import java.io.IOException;

import com.cloudera.framework.testing.TestConstants;
import com.jag.asystem.amodel.avro.Datum;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDatumFactory implements TestConstants {

  private static final Logger LOG = LoggerFactory.getLogger(TestDatumFactory.class);

  @Test
  public void testDatumDefault() throws IOException {
    testDatum("default", DatumFactory.getDatumDefault());
  }

  @Test
  public void testDatumRandom() throws IOException {
    testDatum("random", DatumFactory.getDatumRandom());
  }

  @Test
  public void testDatumRandomIndexed() throws IOException {
    testDatum("indexed-1", DatumFactory.getDatumIndexed(1));
    testDatum("indexed-64", DatumFactory.getDatumIndexed(64));
    testDatum("indexed-2147483647", DatumFactory.getDatumIndexed(2147483647));
  }

  private void testDatum(String type, Datum datum) throws IOException {
    LOG.info("Datum type [" + type + "], serialized size [" + DatumFactory.serializeDatum(datum).length + "] bytes");
  }

}
