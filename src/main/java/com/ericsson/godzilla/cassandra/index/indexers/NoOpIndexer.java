/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index.indexers;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;

/**
 * This indexer does nothing but can be extended
 */
public class NoOpIndexer implements Index.Indexer {

  public static final NoOpIndexer INSTANCE = new NoOpIndexer();

  @Override
  public void begin() {
  }

  @Override
  public void partitionDelete(DeletionTime deletionTime) {
  }

  @Override
  public void rangeTombstone(RangeTombstone tombstone) {
  }

  @Override
  public void insertRow(Row row) {
  }

  @Override
  public void updateRow(Row oldRowData, Row newRowData) {
  }

  @Override
  public void removeRow(Row row) {
  }

  @Override
  public void finish() {
  }
}
