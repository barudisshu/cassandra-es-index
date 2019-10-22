/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index;

import com.google.gson.JsonObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class SearchResult {

  public final List<SearchResultRow> items;
  /** Global ES metadata, like time spent, shards etc ... */
  public final JsonObject metadata;

  public SearchResult(@Nonnull List<SearchResultRow> items, @Nullable JsonObject metadata) {
    this.items = items;
    this.metadata = metadata;
  }
}
