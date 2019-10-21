/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index.monitor;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This interface allows to expose ES client as JMX interface
 */
public interface EsJmxBridgeMXBean {

  String NAME = "com.ericsson.godzilla.cassandra.index.monitor:type=EsJmxBridge";

  /**
   * @return server side timeout for executing requests
   */
  long getTimeout();

  /**
   * @param timeout server side timeout for executing requests
   */
  void setTimeout(long timeout);

  /**
   * @return the cluster status as a Json string
   */
  @Nonnull
  String getClusterState();

  /**
   * @return cluster statistics as a Json string
   */
  @Nonnull
  String getClusterStats();

  /**
   * @return pending tasks as a Json string
   */
  @Nonnull
  String getPendingClusterTasks();

  /**
   * @since 8.5.000.66
   */
  @Nonnull
  String getClusterHealth();

  /**
   * @since 8.5.000.66
   */
  @Nonnull
  String getIndicesStats();

  /**
   * @since 8.5.000.66
   */
  @Nonnull
  String getNodesStats();

  /**
   * Execute a generic command on ES cluster<br> WARNING this changed between 8.5 and 9.0
   *
   * @param methodPath METHOD{space}URI for example "GET ucs/_aliases"
   * @param request    json UTF8 string if needed as a byte array
   * @return UTF8 JSON string or null in some cases
   * @throws IOException is request is not a success, with "404 Not Found" or just "404" is not
   *                     message was returned
   */
  @Nullable
  byte[] execute(@Nonnull String methodPath, @Nullable byte[] request)
      throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, InstantiationException, IOException;

  /**
   * @return true
   */
  boolean connected();

}
