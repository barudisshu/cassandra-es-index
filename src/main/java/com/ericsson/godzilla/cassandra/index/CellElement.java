/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index;

import org.apache.cassandra.utils.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Holds all cell values for ES operations <br>
 * Warning: hashCode() and equals() only uses "name" value.
 */
public class CellElement {

  List<Pair<String, String>> clusteringKeys;
  public String name;
  public String value;
  CollectionValue collectionValue; // can be null if CF has no collections

  public static CellElement create(
      @Nonnull String name, @Nullable String value, @Nullable CollectionValue collectionValue) {
    CellElement elem = new CellElement();
    elem.name = name;
    elem.value = value;
    elem.collectionValue = collectionValue;
    return elem;
  }

  boolean isCollection() {
    return collectionValue != null;
  }

  @Override
  public int hashCode() {
    return name == null ? 0 : name.hashCode();
  }

  @Override
  public boolean equals(@Nullable Object other) {
    return other instanceof CellElement && other.hashCode() == hashCode();
  }

  public static class CollectionValue {
    String name;
    String value;
    CollectionType type;

    @Nonnull
    public static CollectionValue create(
        @Nonnull String name, @Nullable String value, @Nonnull CollectionType type) {
      CollectionValue val = new CollectionValue();
      val.name = name;
      val.value = value;
      val.type = type;
      return val;
    }

    public enum CollectionType {
      MAP,
      /** JSON is used for UDT and tuples */
      JSON,
      SET,
      LIST
    }
  }
}
