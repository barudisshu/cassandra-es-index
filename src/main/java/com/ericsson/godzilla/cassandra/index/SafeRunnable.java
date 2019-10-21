/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index;

import org.slf4j.Logger;

import javax.annotation.Nonnull;

public class SafeRunnable implements Runnable {

  private final Runnable runnable;
  private final String name;
  private final Logger logger;

  public SafeRunnable(@Nonnull Runnable runnable, @Nonnull String name, @Nonnull Logger logger) {
    this.runnable = runnable;
    this.name = name;
    this.logger = logger;
  }

  @Override
  public void run() {
    try {
      runnable.run();
    } catch (Throwable ex) {
      logger.error("Operation '{}' failed: {}", name, ex.getMessage(), ex);
    }
  }
}
