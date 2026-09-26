/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import jakarta.ws.rs.core.SecurityContext;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ResourceConfig;

/** Runs requests through Jersey in memory, without a container or a socket. */
public class InMemoryRequests {
  public static final URI BASE_URI = URI.create("http://localhost:9012/restservices/");

  private final ApplicationHandler handler;

  public InMemoryRequests(final ResourceConfig application) {
    handler = new ApplicationHandler(application);
  }

  public Request request(final String method, final String path) {
    return new Request(method, path);
  }

  public class Request {
    private final ContainerRequest request;
    private Principal user;

    private Request(final String method, final String path) {
      request =
          new ContainerRequest(BASE_URI, BASE_URI.resolve(path), method, new SecurityContext() {
            @Override
            public Principal getUserPrincipal() {
              return user;
            }

            @Override
            public boolean isUserInRole(final String role) {
              return false;
            }

            @Override
            public boolean isSecure() {
              return false;
            }

            @Override
            public String getAuthenticationScheme() {
              return (user == null) ? null : SecurityContext.BASIC_AUTH;
            }
          }, new MapPropertiesDelegate(), handler.getConfiguration());
    }

    public Request header(final String name, final String value) {
      request.header(name, value);
      return this;
    }

    public Request user(final String name) {
      user = () -> name;
      return this;
    }

    public Request body(final String contentType, final String body) {
      request.header("Content-Type", contentType);
      request.setEntityStream(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
      return this;
    }

    public Result send() throws Exception {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      final ContainerResponse response = handler.apply(request, out).get();
      return new Result(response, out.toString(StandardCharsets.UTF_8));
    }
  }

  public static class Result {
    public final ContainerResponse response;
    public final String body;

    private Result(final ContainerResponse response, final String body) {
      this.response = response;
      this.body = body;
    }

    public int status() {
      return response.getStatus();
    }

    public String header(final String name) {
      return response.getHeaderString(name);
    }
  }
}
