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
import org.locationtech.geowave.service.StoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreServiceClient implements StoreService {
  private static final Logger LOGGER = LoggerFactory.getLogger(StoreServiceClient.class);
  private final StoreService storeService;
  // Jersey 2 web resource proxy client doesn't work well with dynamic
  // key-value pair queryparams such as the generic addStore
  private final WebTarget addStoreTarget;

  public StoreServiceClient(final String baseUrl) {
    this(baseUrl, null, null);
  }

  public StoreServiceClient(final String baseUrl, final String user, final String password) {
    final WebTarget target = ClientBuilder.newClient().target(baseUrl);
    storeService = WebResourceFactory.newResource(StoreService.class, target);
    addStoreTarget = createAddStoreTarget(target);
  }

  private static WebTarget createAddStoreTarget(final WebTarget baseTarget) {

    WebTarget addStoreTarget = addPathFromAnnotation(StoreService.class, baseTarget);
    try {
      addStoreTarget =
          addPathFromAnnotation(
              StoreService.class.getMethod(
                  "addStoreReRoute",
                  String.class,
                  String.class,
                  String.class,
                  Map.class),
              addStoreTarget);
    } catch (NoSuchMethodException | SecurityException e) {
      LOGGER.warn("Unable to derive path from method annotations", e);
      // default to hardcoded method path
      addStoreTarget = addStoreTarget.path("/add/{type}");
    }
    return addStoreTarget;
  }

  private static WebTarget addPathFromAnnotation(final AnnotatedElement ae, WebTarget target) {
    final Path p = ae.getAnnotation(Path.class);
    if (p != null) {
      target = target.path(p.value());
    }
    return target;
  }

  public Response listPlugins() {
    final Response resp = storeService.listPlugins();
    resp.bufferEntity();
    return resp;
  }

  public Response version(final String storeName) {
    final Response resp = storeService.version(storeName);
    return resp;
  }

  public Response clear(final String storeName) {
    final Response resp = storeService.clear(storeName);
    return resp;
  }

  public Response addHBaseStore(final String name, final String zookeeper) {

    return addHBaseStore(
        name,
        zookeeper,
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
  public Response addHBaseStore(
      final String name,
      final String zookeeper,
      final Boolean makeDefault,
      final String geowaveNamespace,
      final Boolean disableServiceSide,
      final String coprocessorjar,
      final Boolean persistAdapter,
      final Boolean persistIndex,
      final Boolean persistDataStatistics,
      final Boolean createTable,
      final Boolean useAltIndex,
      final Boolean enableBlockCache) {

    final Response resp =
        storeService.addHBaseStore(
            name,
            zookeeper,
            makeDefault,
            geowaveNamespace,
            disableServiceSide,
            coprocessorjar,
            persistAdapter,
            persistIndex,
            persistDataStatistics,
            createTable,
            useAltIndex,
            enableBlockCache);
    return resp;
  }

  public Response addAccumuloStore(
      final String name,
      final String zookeeper,
      final String instance,
      final String user,
      final String password) {

    return addAccumuloStore(
        name,
        zookeeper,
        instance,
        user,
        password,
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
  public Response addAccumuloStore(
      final String name,
      final String zookeeper,
      final String instance,
      final String user,
      final String password,
      final Boolean makeDefault,
      final String geowaveNamespace,
      final Boolean useLocalityGroups,
      final Boolean persistAdapter,
      final Boolean persistIndex,
      final Boolean persistDataStatistics,
      final Boolean createTable,
      final Boolean useAltIndex,
      final Boolean enableBlockCache) {

    final Response resp =
        storeService.addAccumuloStore(
            name,
            zookeeper,
            instance,
            user,
            password,
            makeDefault,
            geowaveNamespace,
            useLocalityGroups,
            persistAdapter,
            persistIndex,
            persistDataStatistics,
            createTable,
            useAltIndex,
            enableBlockCache);
    return resp;
  }

  public Response addBigTableStore(final String name) {

    return addBigTableStore(
        name,
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
  public Response addBigTableStore(
      final String name,
      final Boolean makeDefault,
      final Integer scanCacheSize,
      final String projectId,
      final String instanceId,
      final String geowaveNamespace,
      final Boolean useLocalityGroups,
      final Boolean persistAdapter,
      final Boolean persistIndex,
      final Boolean persistDataStatistics,
      final Boolean createTable,
      final Boolean useAltIndex,
      final Boolean enableBlockCache) {

    final Response resp =
        storeService.addBigTableStore(
            name,
            makeDefault,
            scanCacheSize,
            projectId,
            instanceId,
            geowaveNamespace,
            useLocalityGroups,
            persistAdapter,
            persistIndex,
            persistDataStatistics,
            createTable,
            useAltIndex,
            enableBlockCache);
    return resp;
  }

  public Response addDynamoDBStore(final String name) {
    return addDynamoDBStore(
        name,
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
  public Response addDynamoDBStore(
      final String name,
      final Boolean makeDefault,
      final String endpoint,
      final String region,
      final Long writeCapacity,
      final Long readCapacity,
      final Integer maxConnections,
      final String protocol,
      final Boolean enableCacheResponseMetadata,
      final String geowaveNamespace,
      final Boolean persistAdapter,
      final Boolean persistIndex,
      final Boolean persistDataStatistics,
      final Boolean createTable,
      final Boolean useAltIndex,
      final Boolean enableBlockCache,
      final Boolean enableServerSideLibrary) {

    final Response resp =
        storeService.addDynamoDBStore(
            name,
            makeDefault,
            endpoint,
            region,
            writeCapacity,
            readCapacity,
            maxConnections,
            protocol,
            enableCacheResponseMetadata,
            geowaveNamespace,
            persistAdapter,
            persistIndex,
            persistDataStatistics,
            createTable,
            useAltIndex,
            enableBlockCache,
            enableServerSideLibrary);
    return resp;
  }

  public Response addCassandraStore(final String name) {
    return addCassandraStore(
        name,
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
  public Response addCassandraStore(
      final String name,
      final Boolean makeDefault,
      final String contactPoint,
      final Integer batchWriteSize,
      final Boolean durableWrites,
      final Integer replicationFactor,
      final String geowaveNamespace,
      final Boolean persistAdapter,
      final Boolean persistIndex,
      final Boolean persistDataStatistics,
      final Boolean createTable,
      final Boolean useAltIndex,
      final Boolean enableBlockCache,
      final Boolean enableServerSideLibrary) {
    final Response resp =
        storeService.addCassandraStore(
            name,
            makeDefault,
            contactPoint,
            batchWriteSize,
            durableWrites,
            replicationFactor,
            geowaveNamespace,
            persistAdapter,
            persistIndex,
            persistDataStatistics,
            createTable,
            useAltIndex,
            enableBlockCache,
            enableServerSideLibrary);
    return resp;
  }

  @Override
  public Response removeStore(final String name) {

    final Response resp = storeService.removeStore(name);
    return resp;
  }

  @Override
  public Response addStoreReRoute(
      final String name,
      final String type,
      final String geowaveNamespace,
      final Map<String, String> additionalQueryParams) {
    WebTarget internalAddStoreTarget = addStoreTarget.resolveTemplate("type", type);
    internalAddStoreTarget = internalAddStoreTarget.queryParam("name", name);
    if ((geowaveNamespace != null) && !geowaveNamespace.isEmpty()) {
      internalAddStoreTarget = internalAddStoreTarget.queryParam("geowaveNamespace", name);
    }
    for (final Entry<String, String> e : additionalQueryParams.entrySet()) {
      if (e.getKey().equals("protocol")) {
        internalAddStoreTarget =
            internalAddStoreTarget.queryParam(e.getKey(), e.getValue().toUpperCase());
      } else {
        internalAddStoreTarget = internalAddStoreTarget.queryParam(e.getKey(), e.getValue());
      }
    }
    return internalAddStoreTarget.request().accept(MediaType.APPLICATION_JSON).method("POST");
  }
}
