/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import org.locationtech.geowave.service.rest.operations.RestOperationStatusMessage;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ServerResource that returns the status of async REST operations submitted to the server */
public class AsyncOperationStatusResource extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOperationStatusResource.class);

  @Get("json")
  public Representation getStatus(final Representation request) {

    final RestOperationStatusMessage status = new RestOperationStatusMessage();
    ConcurrentHashMap<String, Future<?>> opStatuses = null;
    final String id = getQueryValue("id");
    try {
      // look up the operation status
      opStatuses =
          (ConcurrentHashMap<String, Future<?>>) getApplication().getContext().getAttributes().get(
              "asyncOperationStatuses");
      if (opStatuses.get(id) != null) {
        final Future<?> future = opStatuses.get(id);

        if (future.isDone()) {
          status.status = RestOperationStatusMessage.StatusType.COMPLETE;
          status.message = "operation success";
          status.data = future.get();
          opStatuses.remove(id);
        } else {
          status.status = RestOperationStatusMessage.StatusType.RUNNING;
        }
        return new JacksonRepresentation<>(status);
      }
    } catch (final Exception e) {
      LOGGER.error("Error exception: ", e);
      status.status = RestOperationStatusMessage.StatusType.ERROR;
      status.message = "exception occurred";
      status.data = e;
      if (opStatuses != null) {
        opStatuses.remove(id);
      }
      return new JacksonRepresentation<>(status);
    }
    status.status = RestOperationStatusMessage.StatusType.ERROR;
    status.message = "no operation found for ID: " + id;
    return new JacksonRepresentation<>(status);
  }
}
