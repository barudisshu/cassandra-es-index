package com.ericsson.godzilla.cassandra.index;

import com.google.gson.Gson;
import org.junit.Test;

public class BuildTest {

  @Test
  public void ensureJestVersion() {
    new io.searchbox.core.SearchResult(new Gson()).getTotal(); //UCS-5297 will throw method not found on bad versions
  }
}
