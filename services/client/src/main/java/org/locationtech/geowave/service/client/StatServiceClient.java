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
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.locationtech.geowave.service.StatService;

public class StatServiceClient {
  private final StatService statService;

  public StatServiceClient(final String baseUrl) {
    this(baseUrl, null, null);
  }

  public StatServiceClient(final String baseUrl, final String user, final String password) {

    statService =
        WebResourceFactory.newResource(
            StatService.class,
            ClientBuilder.newClient().register(MultiPartFeature.class).target(baseUrl));
  }

  public Response listStats(final String storeName) {

    return listStats(storeName, null, null, null, null, null, null);
  }

  public Response listStats(
      final String storeName,
      final String indexName,
      final String typeName,
      final String fieldName,
      final String tag,
      final String authorizations,
      final Integer limit) {
    final Response resp =
        statService.listStats(
            storeName,
            indexName,
            typeName,
            fieldName,
            tag,
            authorizations,
            limit,
            true);
    return resp;
  }

  public Response recalcStats(final String storeName) {

    return recalcStats(storeName, null, null, null, null, null, true, null);
  }

  public Response recalcStats(
      final String storeName,
      final String statType,
      final String indexName,
      final String typeName,
      final String fieldName,
      final String tag,
      final Boolean all,
      final String authorizations) {

    final Response resp =
        statService.recalcStats(
            storeName,
            statType,
            indexName,
            typeName,
            fieldName,
            tag,
            all,
            authorizations);
    return resp;
  }

  public Response removeStat(
      final String storeName,
      final String statType,
      final String indexName,
      final String typeName,
      final String fieldName,
      final String tag,
      final Boolean all,
      final Boolean force) {

    final Response resp =
        statService.removeStat(
            storeName,
            statType,
            indexName,
            typeName,
            fieldName,
            tag,
            all,
            force);
    return resp;
  }

  public Response removeStat(
      final String storeName,
      final String statType,
      final String typeName,
      final Boolean force) {
    return removeStat(storeName, statType, null, typeName, null, null, true, force);
  }
}
