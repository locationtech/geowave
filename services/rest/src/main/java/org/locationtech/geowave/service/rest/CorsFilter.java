/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.util.Arrays;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

/**
 * Lets pages from any origin call the API without credentials, and pages from the origins listed in
 * {@link GeoWaveRestApplication#CORS_ALLOWED_ORIGINS_PROPERTY} with them. Credentials are not
 * offered to every origin because authentication is the container's: a browser would then let any
 * site a logged-in user visits make calls as that user and read the replies. Preflight requests are
 * answered here, before any other filter sees them.
 */
@PreMatching
public class CorsFilter implements ContainerRequestFilter, ContainerResponseFilter {
  private static final String ALLOWED_METHODS = "GET, POST, PUT, PATCH, DELETE, OPTIONS";

  @Context
  private Configuration configuration;

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
    if (isCredentialedOrigin(origin)) {
      // browsers do not accept a wildcard origin together with credentials
      headers.putSingle("Access-Control-Allow-Origin", origin);
      headers.putSingle("Access-Control-Allow-Credentials", "true");
    } else {
      headers.putSingle("Access-Control-Allow-Origin", "*");
    }
    headers.add("Vary", "Origin");
    if (isPreflight(request)) {
      headers.putSingle("Access-Control-Allow-Methods", ALLOWED_METHODS);
      final String requestedHeaders = request.getHeaderString("Access-Control-Request-Headers");
      if (requestedHeaders != null) {
        headers.putSingle("Access-Control-Allow-Headers", requestedHeaders);
      }
    }
  }

  private boolean isCredentialedOrigin(final String origin) {
    final Object origins =
        configuration.getProperty(GeoWaveRestApplication.CORS_ALLOWED_ORIGINS_PROPERTY);
    return (origins != null)
        && Arrays.stream(origins.toString().split(",")).map(String::trim).anyMatch(origin::equals);
  }

  private static boolean isPreflight(final ContainerRequestContext request) {
    return HttpMethod.OPTIONS.equals(request.getMethod())
        && (request.getHeaderString("Origin") != null)
        && (request.getHeaderString("Access-Control-Request-Method") != null);
  }
}
