/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.util.concurrent.Future;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import org.locationtech.geowave.service.rest.operations.RestOperationStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reports the status of an operation started with runAsync(), always with a 200. */
@Singleton
@Path("v0/operation_status")
public class AsyncOperationStatusResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOperationStatusResource.class);

  private final AsyncOperations asyncOperations;

  @Inject
  public AsyncOperationStatusResource(final AsyncOperations asyncOperations) {
    this.asyncOperations = asyncOperations;
  }

  @GET
  public Response getStatus(@QueryParam("id") final String id) {
    final Future<?> future = asyncOperations.get(id);
    if (future == null) {
      return JsonResponses.of(
          Response.Status.OK,
          JsonResponses.error("no operation found for ID: " + id, null));
    }
    final RestOperationStatusMessage status = new RestOperationStatusMessage();
    if (!future.isDone()) {
      status.status = RestOperationStatusMessage.StatusType.RUNNING;
      return JsonResponses.of(Response.Status.OK, status);
    }
    try {
      status.data = future.get();
      status.status = RestOperationStatusMessage.StatusType.COMPLETE;
      status.message = "operation success";
      return JsonResponses.of(Response.Status.OK, status);
    } catch (final Exception e) {
      LOGGER.error("Error exception: ", e);
      return JsonResponses.of(Response.Status.OK, JsonResponses.error("exception occurred", e));
    } finally {
      asyncOperations.remove(id);
    }
  }
}
