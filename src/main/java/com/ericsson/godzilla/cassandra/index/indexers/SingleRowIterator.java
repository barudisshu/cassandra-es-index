/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index.indexers;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;

public class SingleRowIterator extends AbstractUnfilteredRowIterator {

  private Unfiltered row;

  public SingleRowIterator(
      CFMetaData metadata, Unfiltered row, DecoratedKey key, PartitionColumns columns) {
    super(metadata, key, DeletionTime.LIVE, columns, Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS);
    this.row = row;
  }

  public SingleRowIterator(RowIterator partition, Unfiltered row) {
    super(partition.metadata(), partition.partitionKey(), DeletionTime.LIVE, partition.columns(), partition.staticRow(),
        partition.isReverseOrder(), EncodingStats.NO_STATS);
    this.row = row;
  }

  @Override
  protected synchronized Unfiltered computeNext() {
    try {
      return row == null ? endOfData() : row; //we have to return endOfData() when we're done
    } finally {
      row = null;
    }
  }
}
