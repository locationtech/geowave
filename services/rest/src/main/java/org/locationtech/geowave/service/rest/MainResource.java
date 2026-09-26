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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.locationtech.geowave.service.rest.security.ApiKeyFilter;

/**
 * The main page (essentially index.html): the list of routes and, when the request was
 * authenticated and API keys are enabled, the user's API key.
 */
@Singleton
@Path("/")
public class MainResource {
  private final RestRoutes routes;

  @Inject
  public MainResource(final RestRoutes routes) {
    this.routes = routes;
  }

  @GET
  public Response listResources(@Context final ContainerRequestContext request) {
    final StringBuilder output = new StringBuilder();
    final Object userName = request.getProperty(ApiKeyFilter.USER_NAME_PROPERTY);
    if (userName != null) {
      output.append("<b>Welcome ").append(escape(userName)).append(
          "!</b><br><b>API key:</b> ").append(
              escape(request.getProperty(ApiKeyFilter.API_KEY_PROPERTY))).append("<br><br>");
    }
    output.append("Available Routes:<br>");
    for (final RestRoute route : routes.list()) {
      output.append(route.getPath()).append(" --> ").append(
          route.getOperation().getClass().getName()).append("<br>");
    }
    return Response.ok(output.toString(), MediaType.TEXT_HTML_TYPE).build();
  }

  private static String escape(final Object value) {
    return String.valueOf(value).replace("&", "&amp;").replace("<", "&lt;").replace(
        ">",
        "&gt;").replace("\"", "&quot;");
  }
}
