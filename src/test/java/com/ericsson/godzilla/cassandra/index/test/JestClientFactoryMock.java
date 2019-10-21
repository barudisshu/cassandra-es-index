package com.ericsson.godzilla.cassandra.index.test;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * Created by Jacques-Henri Berthemet on 12/07/2017.
 */
public class JestClientFactoryMock extends JestClientFactory {

  public static final JestClientFactoryMock INSTANCE = new JestClientFactoryMock();
  public static HttpClientConfig httpConfig;
  private final JestClient client = new JestClientMock();

  public JestClient getObject() {
    return client;
  }

  public void setHttpClientConfig(HttpClientConfig httpClientConfig) {
    super.setHttpClientConfig(httpClientConfig);
    httpConfig = httpClientConfig;
  }
}
