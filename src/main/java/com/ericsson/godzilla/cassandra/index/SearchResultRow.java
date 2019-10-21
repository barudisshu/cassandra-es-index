/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index;

import com.google.gson.JsonObject;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

public class SearchResultRow {

  /**
   * Read from ES, contains PK and CLK
   */
  public final String[] primaryKey;
  /**
   * ES Metadata like highlighted fields
   */
  public final JsonObject docMetadata;

  /**
   * Reconstructed Cassandra PK
   */
  public ByteBuffer partitionKey;
  /**
   * Reconstructed Cassandra CLK
   */
  public String[] clusteringKeys;

  public SearchResultRow(@Nonnull String[] primaryKey, @Nonnull JsonObject docMetadata) {
    this.primaryKey = primaryKey;
    this.docMetadata = docMetadata;
  }
}
