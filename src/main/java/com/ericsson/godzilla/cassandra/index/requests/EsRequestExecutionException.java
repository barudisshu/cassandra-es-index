/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index.requests;

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.RequestExecutionException;

public class EsRequestExecutionException extends RequestExecutionException {

  public EsRequestExecutionException(int code, String message) {
    super(ExceptionCode.SERVER_ERROR, code + ":" + message);
  }

  public EsRequestExecutionException(ExceptionCode code, String msg) {
    super(code, msg);
  }
}
