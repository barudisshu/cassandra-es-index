/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index.requests;

import io.searchbox.action.GenericResultAbstractAction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Jest does not support pipeline yet, this is a custom request using Jest API.
 *
 * <p>https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html
 */
public class UpdatePipeline extends GenericResultAbstractAction {

  private final String pipelineId;

  @SuppressWarnings("WeakerAccess") // Don't believe IntelliJ :p
  public UpdatePipeline(@Nonnull UpdatePipeline.Builder builder) {
    super(builder);

    this.pipelineId = builder.pipelineId;
    this.payload = builder.source;
    setURI(buildURI());
  }

  @Override
  protected String buildURI() {
    return "/_ingest/pipeline/" + pipelineId;
  }

  @Override
  public String getRestMethodName() {
    return "PUT";
  }

  public static class Builder extends GenericResultAbstractAction.Builder<UpdatePipeline, Builder> {
    private final String pipelineId;
    private final Object source;

    public Builder(@Nonnull String pipelineId, @Nullable Object source) {
      this.pipelineId = pipelineId;
      this.source = source;
    }

    @Override
    public UpdatePipeline build() {
      return new UpdatePipeline(this);
    }
  }
}
