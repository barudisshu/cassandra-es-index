/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Holds query meta data
 */
public class QueryMetaData {

  //syntax is #options:opt1=val1;opt2=val2#CQL;
  //; and # are not allowed in options
  private static final String META_PREFIX = "#options:";
  private static final String META_SUFFIX = "#";
  private static final String OPTION_SEPARATOR = ",";
  private static final String VALUE_SEPARATOR = "=";

  //Known options
  private static final String LOAD_ROWS = "load-rows";
  private static final String LOAD_SOURCE = "load-source";

  public final String query;
  private final Map<String, String> options = new HashMap<>();

  public QueryMetaData(@Nonnull String queryStr) {
    if (queryStr.startsWith(META_PREFIX)) {
      int endIndex = queryStr.indexOf(META_SUFFIX, META_PREFIX.length());
      query = queryStr.substring(endIndex + 1);

      String optionStr = queryStr.substring(META_PREFIX.length(), endIndex);

      for (String option : optionStr.split(OPTION_SEPARATOR)) {
        String[] items = option.split(VALUE_SEPARATOR);
        options.put(items[0], items[1]);
      }

    } else {
      query = queryStr;
    }
  }

  /**
   * @return (true default) load rows from Cassandra
   */
  public boolean loadRows() {
    String value = options.get(LOAD_ROWS);
    return value == null ? true : Boolean.valueOf(value);
  }

  /**
   * @return (false default) return _source when loading data from ES
   */
  public boolean loadSource() {
    String value = options.get(LOAD_SOURCE);
    return value == null ? false : Boolean.valueOf(value);
  }
}
