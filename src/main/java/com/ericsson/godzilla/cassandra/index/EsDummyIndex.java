/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static java.util.Collections.emptyList;

public class EsDummyIndex implements IndexInterface {

  @Override
  public void init() {}

  @Override
  public SearchResult getMapping(String indexName) {
    return new SearchResult(emptyList(), null);
  }

  @Override
  public SearchResult putMapping(String indexName, String source) {
    return new SearchResult(emptyList(), null);
  }

  @Override
  public void index(
      @Nonnull List<Pair<String, String>> partitionKeys,
      @Nonnull List<CellElement> elements,
      long expirationTime,
      boolean isInsert) {}

  @Override
  public void delete(@Nonnull List<Pair<String, String>> partitionKeys) {}

  @Nullable
  @Override
  public Object flush() {
    return null;
  }

  @Nullable
  @Override
  public Object truncate() {
    return null;
  }

  @Nullable
  @Override
  public Object drop() {
    return null;
  }

  @Nonnull
  @Override
  public SearchResult search(@Nonnull QueryMetaData queryMetaData) {
    return new SearchResult(emptyList(), null);
  }

  @Override
  public void validate(@Nonnull String query) throws InvalidRequestException {}

  @Override
  public void settingsUpdated() {}

  @Override
  public boolean isNewIndex() {
    return false;
  }

  @Override
  public void purgeEmptyIndexes() {}

  @Override
  public void deleteExpired() {}

  @Override
  public void updateIndexConfigOptions() {}

  @Override
  public List<String> getIndexNames() {
    return emptyList();
  }

  @Override
  public void dropIndex(String indexName) {}
}
