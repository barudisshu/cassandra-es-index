/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index.indexers;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;

import java.util.function.BiFunction;

public class NoOpPartitionIterator implements BiFunction<PartitionIterator, ReadCommand, PartitionIterator> {

  public static final NoOpPartitionIterator INSTANCE = new NoOpPartitionIterator();

  @Override
  public PartitionIterator apply(PartitionIterator partitionIterator, ReadCommand command) {
    return partitionIterator;
  }
}
