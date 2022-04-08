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
import org.locationtech.geowave.service.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigServiceClient implements ConfigService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServiceClient.class);
  private final ConfigService configService;

  public ConfigServiceClient(final String baseUrl) {
    this(baseUrl, null, null);
  }

  public ConfigServiceClient(final String baseUrl, final String user, final String password) {
    final WebTarget target = ClientBuilder.newClient().target(baseUrl);
    configService = WebResourceFactory.newResource(ConfigService.class, target);
  }

  @Override
  public Response list(final String filter) {
    final Response resp = configService.list(filter);
    resp.bufferEntity();
    return resp;
  }

  public Response list() {
    return configService.list(null);
  }

  public Response configGeoServer(final String GeoServerURL) {

    return configGeoServer(
        GeoServerURL,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @Override
  public Response configGeoServer(
      final String GeoServerURL,
      final String username,
      final String pass,
      final String workspace,
      final String sslSecurityProtocol,
      final String sslTrustStorePath,
      final String sslTrustStorePassword,
      final String sslTrustStoreType,
      final String sslTruststoreProvider,
      final String sslTrustManagerAlgorithm,
      final String sslTrustManagerProvider,
      final String sslKeyStorePath,
      final String sslKeyStorePassword,
      final String sslKeyStoreProvider,
      final String sslKeyPassword,
      final String sslKeyStoreType,
      final String sslKeyManagerAlgorithm,
      final String sslKeyManagerProvider) {

    final Response resp =
        configService.configGeoServer(
            GeoServerURL,
            username,
            pass,
            workspace,
            sslSecurityProtocol,
            sslTrustStorePath,
            sslTrustStorePassword,
            sslTrustStoreType,
            sslTruststoreProvider,
            sslTrustManagerAlgorithm,
            sslTrustManagerProvider,
            sslKeyStorePath,
            sslKeyStorePassword,
            sslKeyStoreProvider,
            sslKeyPassword,
            sslKeyStoreType,
            sslKeyManagerAlgorithm,
            sslKeyManagerProvider);
    return resp;
  }

  @Override
  public Response configHDFS(final String HDFSDefaultFSURL) {

    final Response resp = configService.configHDFS(HDFSDefaultFSURL);
    return resp;
  }

  public Response set(final String name, final String value) {

    return set(name, value, null);
  }

  @Override
  public Response set(final String name, final String value, final Boolean password) {

    final Response resp = configService.set(name, value, password);
    return resp;
  }
}
