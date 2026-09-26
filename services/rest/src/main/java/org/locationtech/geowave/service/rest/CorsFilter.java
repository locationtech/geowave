/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

/**
 * Lets pages from any origin call the API, with credentials. The origin is echoed back, because
 * browsers do not accept a wildcard origin together with credentials. Preflight requests are
 * answered here, before any other filter sees them.
 */
@PreMatching
public class CorsFilter implements ContainerRequestFilter, ContainerResponseFilter {
  private static final String ALLOWED_METHODS = "GET, POST, PUT, PATCH, DELETE, OPTIONS";

  @Override
  public void filter(final ContainerRequestContext request) {
    if (isPreflight(request)) {
      request.abortWith(Response.ok().build());
    }
  }

  @Override
  public void filter(
      final ContainerRequestContext request,
      final ContainerResponseContext response) {
    final String origin = request.getHeaderString("Origin");
    if (origin == null) {
      return;
    }
    final MultivaluedMap<String, Object> headers = response.getHeaders();
    headers.putSingle("Access-Control-Allow-Origin", origin);
    headers.putSingle("Access-Control-Allow-Credentials", "true");
    headers.add("Vary", "Origin");
    if (isPreflight(request)) {
      headers.putSingle("Access-Control-Allow-Methods", ALLOWED_METHODS);
      final String requestedHeaders = request.getHeaderString("Access-Control-Request-Headers");
      if (requestedHeaders != null) {
        headers.putSingle("Access-Control-Allow-Headers", requestedHeaders);
      }
    }
  }

  private static boolean isPreflight(final ContainerRequestContext request) {
    return HttpMethod.OPTIONS.equals(request.getMethod())
        && (request.getHeaderString("Origin") != null)
        && (request.getHeaderString("Access-Control-Request-Method") != null);
  }
}
