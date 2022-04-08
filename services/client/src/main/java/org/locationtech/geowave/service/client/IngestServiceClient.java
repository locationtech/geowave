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
import org.locationtech.geowave.service.IngestService;

public class IngestServiceClient {
  private final IngestService ingestService;

  public IngestServiceClient(final String baseUrl) {
    this(baseUrl, null, null);
  }

  public IngestServiceClient(final String baseUrl, final String user, final String password) {

    ingestService =
        WebResourceFactory.newResource(
            IngestService.class,
            ClientBuilder.newClient().register(MultiPartFeature.class).target(baseUrl));
  }

  public Response listPlugins() {
    final Response resp = ingestService.listPlugins();
    resp.bufferEntity();
    return resp;
  }

  public Response kafkaToGW(final String storeName, final String indexList) {

    return kafkaToGW(
        storeName,
        indexList,
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

  public Response kafkaToGW(
      final String storeName,
      final String indexList,
      final String kafkaPropertyFile,
      final String visibility,
      final String groupId,
      final String bootstrapServers,
      final String autoOffsetReset,
      final String maxPartitionFetchBytes,
      final String consumerTimeoutMs,
      final Boolean reconnectOnTimeout,
      final Integer batchSize,
      final String extensions,
      final String formats) {

    final Response resp =
        ingestService.kafkaToGW(
            storeName,
            indexList,
            kafkaPropertyFile,
            visibility,
            groupId,
            bootstrapServers,
            autoOffsetReset,
            maxPartitionFetchBytes,
            consumerTimeoutMs,
            reconnectOnTimeout,
            batchSize,
            extensions,
            formats);
    return resp;
  }

  public Response localToGW(
      final String fileOrDirectory,
      final String storeName,
      final String indexList) {

    return localToGW(fileOrDirectory, storeName, indexList, null, null, null, null);
  }

  public Response localToGW(
      final String fileOrDirectory,
      final String storeName,
      final String indexList,
      final Integer threads,
      final String visibility,
      final String extensions,
      final String formats) {

    final Response resp =
        ingestService.localToGW(
            fileOrDirectory,
            storeName,
            indexList,
            threads,
            visibility,
            extensions,
            formats);
    return resp;
  }

  public Response localToHdfs(
      final String fileOrDirectory,
      final String pathToBaseDirectoryToWriteTo) {

    return localToHdfs(fileOrDirectory, pathToBaseDirectoryToWriteTo, null, null);
  }

  public Response localToHdfs(
      final String fileOrDirectory,
      final String pathToBaseDirectoryToWriteTo,
      final String extensions,
      final String formats) {

    final Response resp =
        ingestService.localToHdfs(
            fileOrDirectory,
            pathToBaseDirectoryToWriteTo,
            extensions,
            formats);
    return resp;
  }

  public Response localToKafka(
      final String fileOrDirectory,
      final String kafkaPropertyFile,
      final String bootstrapServers,
      final String retryBackoffMs,
      final String extensions,
      final String formats) {

    final Response resp =
        ingestService.localToKafka(
            fileOrDirectory,
            kafkaPropertyFile,
            bootstrapServers,
            retryBackoffMs,
            extensions,
            formats);
    return resp;
  }

  public Response localToKafka(final String fileOrDirectory, String bootstrapServers) {

    return localToKafka(fileOrDirectory, null, bootstrapServers, null, null, null);
  }

  public Response localToMrGW(
      final String fileOrDirectory,
      final String pathToBaseDirectoryToWriteTo,
      final String storeName,
      final String indexList,
      final String visibility,
      final String jobTrackerHostPort,
      final String resourceManger,
      final String extensions,
      final String formats) {

    final Response resp =
        ingestService.localToMrGW(
            fileOrDirectory,
            pathToBaseDirectoryToWriteTo,
            storeName,
            indexList,
            visibility,
            jobTrackerHostPort,
            resourceManger,
            extensions,
            formats);
    return resp;
  }

  public Response localToMrGW(
      final String fileOrDirectory,
      final String pathToBaseDirectoryToWriteTo,
      final String storeName,
      final String indexList) {

    return localToMrGW(
        fileOrDirectory,
        pathToBaseDirectoryToWriteTo,
        storeName,
        indexList,
        null,
        null,
        null,
        null,
        null);
  }

  public Response mrToGW(
      final String pathToBaseDirectoryToWriteTo,
      final String storeName,
      final String indexList,
      final String visibility,
      final String jobTrackerHostPort,
      final String resourceManger,
      final String extensions,
      final String formats) {

    final Response resp =
        ingestService.mrToGW(
            pathToBaseDirectoryToWriteTo,
            storeName,
            indexList,
            visibility,
            jobTrackerHostPort,
            resourceManger,
            extensions,
            formats);
    return resp;
  }

  public Response mrToGW(
      final String pathToBaseDirectoryToWriteTo,
      final String storeName,
      final String indexList) {

    return mrToGW(pathToBaseDirectoryToWriteTo, storeName, indexList, null, null, null, null, null);
  }

  public Response sparkToGW(
      final String inputDirectory,
      final String storeName,
      final String indexList,
      final String visibility,
      final String appName,
      final String host,
      final String master,
      final Integer numExecutors,
      final Integer numCores,
      final String extensions,
      final String formats) {

    final Response resp =
        ingestService.sparkToGW(
            inputDirectory,
            storeName,
            indexList,
            visibility,
            appName,
            host,
            master,
            numExecutors,
            numCores,
            extensions,
            formats);
    return resp;
  }

  public Response sparkToGW(
      final String inputDirectory,
      final String storeName,
      final String indexList) {
    return sparkToGW(
        inputDirectory,
        storeName,
        indexList,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }
}
