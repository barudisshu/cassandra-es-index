/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index;

import javax.annotation.Nonnull;

/** Handles changes in index names, updates and scheduled operations */
public interface IndexManager {
  /**
   * Alias name
   *
   * @return alias name
   */
  @Nonnull
  String getAliasName();

  /**
   * Current index name
   *
   * @return current (active) index name
   */
  @Nonnull
  String getCurrentName();

  /** Stop index processing */
  void stop();

  /** Check index segmentation processing */
  void checkForUpdate();

  /** Reload index manager options */
  void updateOptions();

  /** Is separated TTL field must be created in every document */
  boolean isTTLFieldRequired();
}
