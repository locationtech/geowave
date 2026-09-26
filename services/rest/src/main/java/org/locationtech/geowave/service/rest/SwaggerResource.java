/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.net.URI;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.locationtech.geowave.core.cli.VersionUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Serves the Swagger 2.0 description of the API. The host and base path are the ones the request
 * was made to, unless the host_port property names the host.
 */
@Singleton
@Path("api")
public class SwaggerResource {
  private final SwaggerApiParser apiParser;

  @Inject
  public SwaggerResource(final RestRoutes routes) {
    apiParser =
        new SwaggerApiParser(
            VersionUtils.getVersion(),
            "GeoWave API",
            "REST API for GeoWave CLI commands");
    for (final RestRoute route : routes.list()) {
      apiParser.addRoute(route);
    }
  }

  @GET
  public Response swagger(
      @Context final UriInfo uriInfo,
      @Context final Configuration configuration) throws JsonProcessingException {
    final URI baseUri = uriInfo.getBaseUri();
    final Object hostPort = configuration.getProperty(GeoWaveRestApplication.HOST_PORT_PROPERTY);
    final String basePath = baseUri.getPath();
    final String swagger =
        JsonResponses.MAPPER.writeValueAsString(
            apiParser.getSwagger(
                (hostPort != null) ? hostPort.toString() : baseUri.getAuthority(),
                (basePath.length() > 1) && basePath.endsWith("/")
                    ? basePath.substring(0, basePath.length() - 1)
                    : basePath,
                baseUri.getScheme()));
    return Response.ok(swagger, MediaType.APPLICATION_JSON_TYPE).build();
  }
}
