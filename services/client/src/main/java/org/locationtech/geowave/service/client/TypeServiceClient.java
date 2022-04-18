/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.locationtech.geowave.service.TypeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeServiceClient implements TypeService {
  private static final Logger LOGGER = LoggerFactory.getLogger(TypeServiceClient.class);
  private final TypeService typeService;

  public TypeServiceClient(final String baseUrl) {
    this(baseUrl, null, null);
  }

  public TypeServiceClient(final String baseUrl, final String user, final String password) {
    final WebTarget target = ClientBuilder.newClient().target(baseUrl);
    typeService = WebResourceFactory.newResource(TypeService.class, target);
  }

  public Response list(final String storeName) {
    final Response resp = typeService.list(storeName);
    return resp;
  }

  public Response remove(final String storeName, final String typeName) {
    final Response resp = typeService.remove(storeName, typeName);
    return resp;
  }

  public Response describe(final String storeName, final String typeName) {
    final Response resp = typeService.describe(storeName, typeName);
    return resp;
  }

}
