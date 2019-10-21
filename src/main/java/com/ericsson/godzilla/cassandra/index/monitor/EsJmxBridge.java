/*
* Copyright Ericsson AB 2019 - All Rights Reserved.
* The copyright to the computer program(s) herein is the property of Ericsson AB.
* The programs may be used and/or copied only with written permission from Ericsson AB
* or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
*/
package com.ericsson.godzilla.cassandra.index.monitor;

import com.ericsson.godzilla.cassandra.index.requests.GenericRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.NodesStats;
import io.searchbox.cluster.PendingClusterTasks;
import io.searchbox.cluster.State;
import io.searchbox.cluster.Stats;

import static java.nio.charset.StandardCharsets.UTF_8;

public class EsJmxBridge implements EsJmxBridgeMXBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(EsJmxBridge.class);

  private final JestClient client;

  public EsJmxBridge(JestClient client)
      throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
    this.client = client;

    LOGGER.info("Registering ES JMX bridge");
    //Get the MBean server and register the MBean
    ManagementFactory.getPlatformMBeanServer().registerMBean(this, new ObjectName(NAME));
    LOGGER.info("Registration of '{}' successful", NAME);
  }

  @Override
  public long getTimeout() {
    return -1;
  }

  @Override
  public void setTimeout(long timeout) {
  }

  @Override
  @Nonnull
  public String getClusterState() {
    return execute(new State.Builder().build()).getJsonString();
  }

  @Override
  @Nonnull
  public String getClusterStats() {
    return execute(new Stats.Builder().build()).getJsonString();
  }

  @Override
  @Nonnull
  public String getPendingClusterTasks() {
    return execute(new PendingClusterTasks.Builder().build()).getJsonString();
  }

  @Override
  @Nonnull
  public String getClusterHealth() {
    return execute(new Health.Builder().build()).getJsonString();
  }

  @Override
  @Nonnull
  public String getIndicesStats() {
    return execute(new io.searchbox.indices.Stats.Builder().build()).getJsonString();
  }

  @Override
  @Nonnull
  public String getNodesStats() {
    return execute(new NodesStats.Builder().build()).getJsonString();
  }

  @Override
  public boolean connected() {
    return true;
  }

  @Override
  @Nullable
  public byte[] execute(@Nonnull String methodPath, @Nullable byte[] request) throws IOException {
    int pos = methodPath.indexOf(' ');
    String method = methodPath.substring(0, pos);
    String url = methodPath.substring(pos + 1);
    String payload = (request == null || request.length == 0) ? null : new String(request, UTF_8);

    JestResult result = execute(new GenericRequest(method, url, payload));

    if (!result.isSucceeded()) {
      throw result.getErrorMessage() == null
          ? new IOException(String.valueOf(result.getResponseCode()))
          : new IOException(result.getResponseCode() + " " + result.getErrorMessage());
    }

    return result.getJsonString() == null ? new byte[0] : result.getJsonString().getBytes(UTF_8);
  }

  private <T extends JestResult> T execute(Action<T> request) {
    try {
      return client.execute(request);
    } catch (IOException e) {
      LOGGER.error("Failed to send ES request:{}", request, e);
      throw new RuntimeException(e);
    }
  }
}
