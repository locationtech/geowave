/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.locationtech.geowave.service.rest.operations.RestOperationStatusMessage;
import org.locationtech.geowave.service.rest.operations.RestOperationStatusMessage.StatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/** Builds the JSON responses that carry a {@link RestOperationStatusMessage}. */
public final class JsonResponses {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonResponses.class);

  // Operation results and caught exceptions are serialized as beans, and some have no properties.
  static final ObjectMapper MAPPER =
      new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private JsonResponses() {}

  public static Response of(
      final Response.StatusType status,
      final RestOperationStatusMessage message) {
    try {
      return json(status, MAPPER.writeValueAsString(message));
    } catch (final JsonProcessingException e) {
      LOGGER.error("Unable to serialize the response", e);
      try {
        return json(
            Response.Status.INTERNAL_SERVER_ERROR,
            MAPPER.writeValueAsString(
                error("Unable to serialize the response: " + e.getOriginalMessage(), null)));
      } catch (final JsonProcessingException unexpected) {
        throw new IllegalStateException(unexpected);
      }
    }
  }

  public static RestOperationStatusMessage error(final String message, final Object data) {
    final RestOperationStatusMessage error = new RestOperationStatusMessage();
    error.status = StatusType.ERROR;
    error.message = message;
    error.data = data;
    return error;
  }

  private static Response json(final Response.StatusType status, final String json) {
    return Response.status(status).type(MediaType.APPLICATION_JSON_TYPE).entity(json).build();
  }
}
