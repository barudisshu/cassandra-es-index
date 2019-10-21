/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index.requests;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.searchbox.client.JestClientFactory;

/**
 * This is useful for tests
 */
public class ElasticClientFactory {

  public static final String REST_CLIENT_CLASS = "io.searchbox.client.JestClientFactory";
  private static JestClientFactory factory;

  /**
   * @return a new instance of JestClientFactory or whatever was set using setJestClientFactory()
   */
  @Nonnull
  public static JestClientFactory getJestClientFactory() {
    return factory == null ? new JestClientFactory() : factory;
  }


  /**
   * @param newFactory a factory that should be used by all classes that need a JestClientFactory,
   *                   usually for testing
   * @return the previous value
   */
  @Nullable
  public static JestClientFactory setJestClientFactory(@Nullable JestClientFactory newFactory) {
    JestClientFactory prev = factory;
    factory = newFactory;
    return prev;
  }
}
