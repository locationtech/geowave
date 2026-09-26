/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.service.rest.security.ApiKeyFeature;

/**
 * The REST services: one route for every concrete {@link ServiceEnabledCommand}, plus the main
 * page, the Swagger description at /api, file upload and the status of asynchronous operations.
 *
 * <p> It is configured through the properties below, which a servlet deployment sets as init or
 * context parameters in web.xml.
 */
public class GeoWaveRestApplication extends ResourceConfig {
  /**
   * The GeoWave config file that requests use unless they name one. Without it, the default config
   * file of the user running the container is used.
   */
  public static final String CONFIG_FILE_PROPERTY = "config_file";
  /**
   * The host and port to put in the Swagger description, for example behind a proxy. Without it,
   * the host of the request is used.
   */
  public static final String HOST_PORT_PROPERTY = "host_port";
  /**
   * A SQLite database file, created if needed, that holds API keys. When it is set, every request
   * under /v0 needs a valid apiKey query parameter.
   */
  public static final String API_KEY_DB_PROPERTY = "api_key_db";
  /**
   * Comma-separated origins (for example https://app.example.com) whose pages may call the API with
   * the user's credentials. Pages from any other origin may call it only without credentials.
   */
  public static final String CORS_ALLOWED_ORIGINS_PROPERTY = "cors_allowed_origins";

  public GeoWaveRestApplication() {
    this(RestRoutes.find());
  }

  public GeoWaveRestApplication(final RestRoutes routes) {
    final AsyncOperations asyncOperations = new AsyncOperations();
    // Resources are registered as classes: Jersey 3.1 warns that a resource instance "will be
    // ignored" as a provider, although it serves it.
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(routes).to(RestRoutes.class);
        bind(asyncOperations).to(AsyncOperations.class);
      }
    });
    register(MainResource.class);
    register(SwaggerResource.class);
    register(OperationResource.class);
    register(AsyncOperationStatusResource.class);
    register(FileUploadResource.class);
    register(CorsFilter.class);
    register(MultiPartFeature.class);
    register(ApiKeyFeature.class);
    register(new ContainerLifecycleListener() {
      @Override
      public void onStartup(final Container container) {}

      @Override
      public void onReload(final Container container) {}

      @Override
      public void onShutdown(final Container container) {
        asyncOperations.shutdown();
      }
    });
    // WADL generation needs JAXB, which is not on the classpath, and nothing uses it.
    property(ServerProperties.WADL_FEATURE_DISABLE, true);
  }
}
