/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index.requests;

import io.searchbox.action.GenericResultAbstractAction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class GenericRequest extends GenericResultAbstractAction {

  private final String method;
  private final String url;

  public GenericRequest(@Nonnull String method, @Nonnull String url, @Nullable Object payload) {
    super();
    this.payload = payload;
    this.method = method;
    this.url = url;
    setURI(buildURI());
  }

  @Override
  public String getRestMethodName() {
    return method;
  }

  @Override
  protected String buildURI() {
    return url;
  }
}
