/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.security;

import java.security.Principal;
import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import org.locationtech.geowave.service.rest.JsonResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Requires a valid apiKey query parameter on every request under /v0, and rejects the rest with a
 * 401. On other paths, a user the container has authenticated is given their key, as request
 * properties that the main page shows.
 */
@Priority(Priorities.AUTHENTICATION)
public class ApiKeyFilter implements ContainerRequestFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiKeyFilter.class);
  public static final String API_KEY_PARAMETER = "apiKey";
  public static final String USER_NAME_PROPERTY = ApiKeyFilter.class.getName() + ".userName";
  public static final String API_KEY_PROPERTY = ApiKeyFilter.class.getName() + ".apiKey";

  private final ApiKeyStore store;

  public ApiKeyFilter(final ApiKeyStore store) {
    this.store = store;
  }

  @Override
  public void filter(final ContainerRequestContext request) {
    if (isApiRequest(request)) {
      final String apiKey = request.getUriInfo().getQueryParameters().getFirst(API_KEY_PARAMETER);
      if ((apiKey == null) || apiKey.isEmpty()) {
        reject(request, "apiKey is required");
      } else if (!store.hasKey(apiKey)) {
        reject(request, "apiKey is invalid");
      }
      return;
    }
    final Principal user = request.getSecurityContext().getUserPrincipal();
    if (user != null) {
      final String apiKey = store.getOrCreateKey(user.getName());
      if (apiKey != null) {
        request.setProperty(USER_NAME_PROPERTY, user.getName());
        request.setProperty(API_KEY_PROPERTY, apiKey);
      }
    }
  }

  private static boolean isApiRequest(final ContainerRequestContext request) {
    String path = request.getUriInfo().getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return path.equals("v0") || path.startsWith("v0/");
  }

  private static void reject(final ContainerRequestContext request, final String message) {
    LOGGER.warn("Rejected " + request.getUriInfo().getPath() + ": " + message);
    request.abortWith(
        JsonResponses.of(Response.Status.UNAUTHORIZED, JsonResponses.error(message, null)));
  }
}
