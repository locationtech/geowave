/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;
import org.locationtech.geowave.core.cli.utils.InstantiationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves every {@link RestRoute}: the path is looked up exactly, and the request is handed to a
 * fresh instance of the route's operation. The fixed paths of the other resources take precedence
 * over this one's template.
 *
 * <p> The resources declare no media type they produce and answer JSON whatever a request accepts,
 * as the Restlet services did: services/client's StatService asks for text/plain.
 */
@Singleton
@Path("{route: .+}")
public class OperationResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperationResource.class);

  private final RestRoutes routes;
  private final AsyncOperations asyncOperations;

  @Context
  private Configuration configuration;

  @Inject
  public OperationResource(final RestRoutes routes, final AsyncOperations asyncOperations) {
    this.routes = routes;
    this.asyncOperations = asyncOperations;
  }

  @GET
  public Response get(
      @PathParam("route") final String route,
      @Context final UriInfo uriInfo,
      @Context final HttpHeaders headers) {
    return handle(HttpMethod.GET, route, uriInfo, headers, null);
  }

  @POST
  public Response post(
      @PathParam("route") final String route,
      @Context final UriInfo uriInfo,
      @Context final HttpHeaders headers,
      final String body) {
    return handle(HttpMethod.POST, route, uriInfo, headers, body);
  }

  @PUT
  public Response put(
      @PathParam("route") final String route,
      @Context final UriInfo uriInfo,
      @Context final HttpHeaders headers,
      final String body) {
    return handle(HttpMethod.PUT, route, uriInfo, headers, body);
  }

  @PATCH
  public Response patch(
      @PathParam("route") final String route,
      @Context final UriInfo uriInfo,
      @Context final HttpHeaders headers,
      final String body) {
    return handle(HttpMethod.PATCH, route, uriInfo, headers, body);
  }

  @DELETE
  public Response delete(
      @PathParam("route") final String route,
      @Context final UriInfo uriInfo,
      @Context final HttpHeaders headers,
      final String body) {
    return handle(HttpMethod.DELETE, route, uriInfo, headers, body);
  }

  private Response handle(
      final HttpMethod method,
      final String path,
      final UriInfo uriInfo,
      final HttpHeaders headers,
      final String body) {
    final RestRoute route = routes.get(path);
    if (route == null) {
      throw new NotFoundException();
    }
    final ServiceEnabledCommand<?> operation;
    try {
      operation = InstantiationUtils.newInstance(route.getOperation().getClass());
    } catch (InstantiationException | IllegalAccessException e) {
      LOGGER.error("Unable to instantiate Service Resource", e);
      return JsonResponses.of(
          Response.Status.INTERNAL_SERVER_ERROR,
          JsonResponses.error("exception occurred", e));
    }
    return new GeoWaveOperationServiceWrapper<>(
        operation,
        (String) configuration.getProperty(GeoWaveRestApplication.CONFIG_FILE_PROPERTY),
        asyncOperations).handle(method, headers.getMediaType(), body, uriInfo.getQueryParameters());
  }
}
