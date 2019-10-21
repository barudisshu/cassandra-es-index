package com.ericsson.godzilla.cassandra.index;

import org.junit.Assert;
import org.junit.Test;

public class QueryMetaDataTest {

  @Test
  public void testWithOptionFalse() {
    QueryMetaData meta = new QueryMetaData("#options:load-rows=false#plop=42");

    Assert.assertEquals("plop=42", meta.query);
    Assert.assertFalse(meta.loadRows());
  }

  @Test
  public void testWithOptionTrue() {
    QueryMetaData meta = new QueryMetaData("#options:load-rows=true#plop=42###");

    Assert.assertEquals("plop=42###", meta.query);
    Assert.assertTrue(meta.loadRows());
  }

  @Test
  public void testWithExtraOptionTrue() {
    QueryMetaData meta = new QueryMetaData("#options:load-rows=true,number=42#plop=42###");

    Assert.assertEquals("plop=42###", meta.query);
    Assert.assertTrue(meta.loadRows());
  }

  @Test
  public void testWithoutOption() {
    QueryMetaData meta = new QueryMetaData("Text=42#plop");

    Assert.assertEquals("Text=42#plop", meta.query);
    Assert.assertTrue(meta.loadRows());
  }

}
