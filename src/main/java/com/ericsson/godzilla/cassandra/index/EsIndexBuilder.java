/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index;

import com.google.common.base.Stopwatch;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Index building task that reads all live SSTables and index the content
 */
public class EsIndexBuilder extends SecondaryIndexBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(EsIndexBuilder.class);

  private final UUID compactionId = UUIDGen.getTimeUUID();
  private final EsSecondaryIndex index;
  private final Collection<SSTableReader> ssTables;
  private final long total;
  private long processed = 0;

  EsIndexBuilder(EsSecondaryIndex index) {
    this(index, index.baseCfs.getLiveSSTables());
  }

  EsIndexBuilder(EsSecondaryIndex index, Collection<SSTableReader> ssTables) {
    this.index = index;
    this.ssTables = ssTables;
    this.total = ssTables.stream().mapToLong(SSTableReader::getTotalRows).sum();
  }

  @Override
  public void build() {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    LOGGER.info("{} build {} starting on {} ssTables with {} rows to index", index.name, compactionId, ssTables.size(), total);

    if (index.indexConfig.isTruncateBeforeRebuild()) {
      index.esIndex.truncate();
    }

    //For each of the SSTables, we get a partition scanner, for each partition we get its row and we index it
    ssTables.forEach(ssTableReader -> ssTableReader.getScanner()
        .forEachRemaining(partition -> {
              partition.forEachRemaining(row -> {
                if (isStopRequested()) {
                  LOGGER.warn("{} build {} stop requested {}/{} rows", index.name, compactionId, processed, total);
                  throw new CompactionInterruptedException(getCompactionInfo());
                }

                DecoratedKey key = partition.partitionKey();
                if (row instanceof Row) { //not sure what else it could be
                  index.index(key, (Row) row, null, FBUtilities.nowInSeconds());
                } else {
                  LOGGER.warn("{} build {} skipping unsupported {} {}", index.name, compactionId, row.getClass().getName(), key);
                }
                processed++;
              });
            }
        )
    );

    LOGGER.info("{} build {} completed in {} minutes for {} rows", index.name, compactionId, stopwatch.elapsed(TimeUnit.MINUTES), total);
  }

  public CompactionInfo getCompactionInfo() {
    return new CompactionInfo(null, OperationType.INDEX_BUILD, processed, total, null, compactionId);
  }
}
