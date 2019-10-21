/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index.indexers;

import com.ericsson.godzilla.cassandra.index.EsSecondaryIndex;
import com.ericsson.godzilla.cassandra.index.SearchResult;
import com.ericsson.godzilla.cassandra.index.SearchResultRow;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Iterator;

/**
 * This iterator does not load data from Cassandra and will only return PKs and ES metadata
 */
public class FakePartitionIterator implements UnfilteredPartitionIterator {

  private final Iterator<SearchResultRow> esResultIterator;
  private final ColumnFamilyStore baseCfs;
  private final ReadCommand command;
  private final String searchId;
  private final PartitionColumns returnedColumns;

  public FakePartitionIterator(EsSecondaryIndex index, SearchResult searchResult, ReadCommand command, String searchId) {
    this.baseCfs = index.baseCfs;
    this.esResultIterator = searchResult.items.iterator();
    this.command = command;
    this.searchId = searchId;
    this.returnedColumns = PartitionColumns.builder().build();
    Tracing.trace("ESI {} FakePartitionIterator initialized", searchId);
  }

  @Override
  public boolean isForThrift() {
    return command.isForThrift();
  }

  @Override
  public CFMetaData metadata() {
    return command.metadata();
  }

  @Override
  public void close() {
    Tracing.trace("ESI {} FakePartitionIterator closed", searchId);
  }

  @Override
  public boolean hasNext() {
    return esResultIterator.hasNext();
  }

  @Override
  public UnfilteredRowIterator next() {
    if (!esResultIterator.hasNext()) {
      return null;
    }

    //Build the minimum row
    Row.Builder rowBuilder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
    rowBuilder.newRow(Clustering.EMPTY); //FIXME support for clustering
    rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.EMPTY);
    rowBuilder.addRowDeletion(Row.Deletion.LIVE);

    SearchResultRow esResult = esResultIterator.next();

    //And PK value
    DecoratedKey partitionKey = baseCfs.getPartitioner().decorateKey(esResult.partitionKey);
    return new SingleRowIterator(baseCfs.metadata, rowBuilder.build(), partitionKey, returnedColumns);
  }
}
