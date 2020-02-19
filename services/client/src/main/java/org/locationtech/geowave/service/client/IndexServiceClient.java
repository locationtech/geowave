/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.client;

import java.lang.reflect.AnnotatedElement;
import java.util.Map;
import java.util.Map.Entry;
import javax.ws.rs.Path;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.locationtech.geowave.service.IndexService;
import org.locationtech.geowave.service.StoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexServiceClient implements IndexService {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexServiceClient.class);
  private final IndexService indexService;
  private final WebTarget addIndexTarget;

  public IndexServiceClient(final String baseUrl) {
    this(baseUrl, null, null);
  }

  public IndexServiceClient(final String baseUrl, final String user, final String password) {
    final WebTarget target = ClientBuilder.newClient().target(baseUrl);
    indexService = WebResourceFactory.newResource(IndexService.class, target);
    addIndexTarget = createAddIndexTarget(target);
  }

  private static WebTarget createAddIndexTarget(final WebTarget baseTarget) {

    WebTarget addIndexTarget = addPathFromAnnotation(StoreService.class, baseTarget);
    try {
      addIndexTarget =
          addPathFromAnnotation(
              IndexService.class.getMethod(
                  "addIndexReRoute",
                  String.class,
                  String.class,
                  Map.class),
              addIndexTarget);
    } catch (NoSuchMethodException | SecurityException e) {
      LOGGER.warn("Unable to derive path from method annotations", e);
      // default to hardcoded method path
      addIndexTarget = addIndexTarget.path("/add/{type}");
    }
    return addIndexTarget;
  }

  private static WebTarget addPathFromAnnotation(final AnnotatedElement ae, WebTarget target) {
    final Path p = ae.getAnnotation(Path.class);
    if (p != null) {
      target = target.path(p.value());
    }
    return target;
  }

  public Response listPlugins() {
    final Response resp = indexService.listPlugins();
    resp.bufferEntity();
    return resp;
  }

  public Response listIndices(final String storeName) {
    final Response resp = indexService.listIndices(storeName);
    return resp;
  }

  public Response addSpatialIndex(final String storeName, final String indexName) {
    return addSpatialIndex(storeName, indexName, null, null, null, null, null);
  }

  @Override
  public Response addSpatialIndex(
      final String storeName,
      final String indexName,
      final Boolean makeDefault,
      final Integer numPartitions,
      final String partitionStrategy,
      final Boolean storeTime,
      final String crs) {

    final Response resp =
        indexService.addSpatialIndex(
            storeName,
            indexName,
            makeDefault,
            numPartitions,
            partitionStrategy,
            storeTime,
            crs);
    return resp;
  }

  public Response addSpatialTemporalIndex(final String storeName, final String indexName) {
    return addSpatialTemporalIndex(storeName, indexName, null, null, null, null, null, null, null);
  }

  @Override
  public Response addSpatialTemporalIndex(
      final String storeName,
      final String indexName,
      final Boolean makeDefault,
      final Integer numPartitions,
      final String partitionStrategy,
      final String periodicity,
      final String bias,
      final Long maxDuplicates,
      final String crs) {

    final Response resp =
        indexService.addSpatialTemporalIndex(
            storeName,
            indexName,
            makeDefault,
            numPartitions,
            partitionStrategy,
            periodicity,
            bias,
            maxDuplicates,
            crs);
    return resp;
  }

  @Override
  public Response removeIndex(final String storeName, final String indexName) {

    final Response resp = indexService.removeIndex(storeName, indexName);
    return resp;
  }

  @Override
  public Response addIndexReRoute(
      final String storeName,
      final String indexName,
      final String type,
      final Map<String, String> additionalQueryParams) {
    WebTarget internalAddStoreTarget = addIndexTarget.resolveTemplate("type", type);
    internalAddStoreTarget = internalAddStoreTarget.queryParam("storeName", storeName);
    internalAddStoreTarget = internalAddStoreTarget.queryParam("indexName", indexName);
    for (final Entry<String, String> e : additionalQueryParams.entrySet()) {
      internalAddStoreTarget = internalAddStoreTarget.queryParam(e.getKey(), e.getValue());
    }
    return internalAddStoreTarget.request().accept(MediaType.APPLICATION_JSON).method("POST");
  }
}
