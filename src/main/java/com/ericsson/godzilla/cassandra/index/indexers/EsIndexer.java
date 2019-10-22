/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index.indexers;

import com.ericsson.godzilla.cassandra.index.EsSecondaryIndex;
import com.google.common.base.Stopwatch;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** This indexer handles inserts and updates but discards deletes */
public class EsIndexer extends NoOpIndexer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EsIndexer.class);

  private final EsSecondaryIndex index;
  private final DecoratedKey key;
  private final int nowInSec;
  private final String id;
  private final boolean delete;

  public EsIndexer(EsSecondaryIndex index, DecoratedKey key, int nowInSec, boolean withDelete) {
    this.key = key;
    this.nowInSec = nowInSec;
    this.index = index;
    this.id = ByteBufferUtil.bytesToHex(key.getKey());
    this.delete = withDelete;
  }

  // see https://opencredo.com/cassandra-tombstones-common-issues/
  // UCS-5008, WCC-1344 rangeTombstone and removeRow do nothing

  @Override
  public void insertRow(Row row) {
    Stopwatch time = Stopwatch.createStarted();
    index.index(key, row, null, nowInSec);
    LOGGER.debug("{} insertRow {} took {}ms", index.name, id, time.elapsed(TimeUnit.MILLISECONDS));
  }

  @Override
  public void updateRow(Row oldRowData, Row newRowData) {
    Stopwatch time = Stopwatch.createStarted();
    index.index(key, newRowData, oldRowData, nowInSec);
    LOGGER.debug("{} updateRow {} took {}ms", index.name, id, time.elapsed(TimeUnit.MILLISECONDS));
  }

  @Override
  public void partitionDelete(DeletionTime deletionTime) {
    if (delete) {
      Stopwatch time = Stopwatch.createStarted();
      index.delete(key);
      LOGGER.debug(
          "{} partitionDelete {} took {}ms", index.name, id, time.elapsed(TimeUnit.MILLISECONDS));
    }
  }
}
