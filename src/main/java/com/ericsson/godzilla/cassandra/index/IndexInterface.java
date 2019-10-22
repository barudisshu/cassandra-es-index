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
import java.io.IOException;
import java.util.List;

public interface IndexInterface {

  /** Perform any initialization work */
  void init();

  /** Reads the getMapping */
  SearchResult getMapping(String index);

  /** Updates the getMapping */
  SearchResult putMapping(String index, String source);

  /**
   * Index a new document
   *
   * @param partitionKeys not null, not empty
   * @param elements not null, not empty
   * @param expirationTime in seconds
   * @param isInsert if false will use update
   * @throws IOException if something goes wrong
   */
  void index(
      @Nonnull List<Pair<String, String>> partitionKeys,
      @Nonnull List<CellElement> elements,
      long expirationTime,
      boolean isInsert)
      throws IOException;

  /**
   * Delete the corresponding document
   *
   * @param partitionKeys not null, not empty
   */
  void delete(@Nonnull List<Pair<String, String>> partitionKeys);

  /** Flush all data from memory to disk */
  @Nullable
  Object flush();

  /** Truncate the index */
  @Nullable
  Object truncate();

  /** Drop the index */
  @Nullable
  Object drop();

  /**
   * Find results matching the query string with matching partitionKeys, clusteringColumnsNames
   *
   * @param queryMetaData not null, the query to execute
   * @return not null, SearchResult which contains a list SearchResultRows and result metadata as
   *     Json string
   */
  @Nonnull
  SearchResult search(@Nonnull QueryMetaData queryMetaData);

  /** @param query the query to validate */
  void validate(@Nonnull String query) throws InvalidRequestException;

  /** Something may have changed in the configuration */
  void settingsUpdated();

  /** @return true _once_ if index was newly created */
  boolean isNewIndex();

  /** Delete indexes that have no documents */
  void purgeEmptyIndexes();

  /** Enforce Cassandra TTL expiration */
  void deleteExpired();

  /** Reload options from index config */
  void updateIndexConfigOptions();

  /**
   * Delete index with specified name
   *
   * @param indexName index name
   */
  void dropIndex(String indexName);

  /** Returns list of index names for current alias */
  List<String> getIndexNames();
}
