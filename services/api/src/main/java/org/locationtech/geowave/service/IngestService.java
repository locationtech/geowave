/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/ingest")
public interface IngestService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/listplugins")
  public Response listPlugins();

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/kafkaToGW")
  public Response kafkaToGW(
      @QueryParam("storeName") String storeName,
      @QueryParam("indexList") String indexList, // Array of
      // Strings
      @QueryParam("kafkaPropertyFile") String kafkaPropertyFile,
      @QueryParam("visibility") String visibility,
      @QueryParam("groupId") String groupId,
      @QueryParam("bootstrapServers") String bootstrapServers,
      @QueryParam("autoOffsetReset") String autoOffsetReset,
      @QueryParam("maxPartitionFetchBytes") String maxPartitionFetchBytes,
      @QueryParam("consumerTimeoutMs") String consumerTimeoutMs,
      @QueryParam("reconnectOnTimeout") Boolean reconnectOnTimeout,
      @QueryParam("batchSize") Integer batchSize,
      @QueryParam("extensions") String extensions, // Array
      // of
      // Strings
      @QueryParam("formats") String formats);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/localToGW")
  public Response localToGW(
      @QueryParam("fileOrDirectory") String fileOrDirectory,
      @QueryParam("storeName") String storeName,
      @QueryParam("indexList") String indexList, // Array of
      // Strings
      @QueryParam("threads") Integer threads,
      @QueryParam("visibility") String visibility,
      @QueryParam("extensions") String extensions, // Array of Strings
      @QueryParam("formats") String formats);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/localToHdfs")
  public Response localToHdfs(
      @QueryParam("fileOrDirectory") String fileOrDirectory,
      @QueryParam("pathToBaseDirectoryToWriteTo") String pathToBaseDirectoryToWriteTo,
      @QueryParam("extensions") String extensions, // Array of Strings
      @QueryParam("formats") String formats);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/localToKafka")
  public Response localToKafka(
      @QueryParam("fileOrDirectory") String fileOrDirectory,
      @QueryParam("kafkaPropertyFile") String kafkaPropertyFile,
      @QueryParam("bootstrapServers") String bootstrapServers,
      @QueryParam("retryBackoffMs") String retryBackoffMs,
      @QueryParam("extensions") String extensions, // Array of Strings
      @QueryParam("formats") String formats);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/localToMrGW")
  public Response localToMrGW(
      @QueryParam("fileOrDirectory") String fileOrDirectory,
      @QueryParam("pathToBaseDirectoryToWriteTo") String pathToBaseDirectoryToWriteTo,
      @QueryParam("storeName") String storeName,
      @QueryParam("indexList") String indexList, // Array of
      // Strings
      @QueryParam("visibility") String visibility,
      @QueryParam("jobTrackerHostPort") String jobTrackerHostPort,
      @QueryParam("resourceManger") String resourceManger,
      @QueryParam("extensions") String extensions, // Array of Strings
      @QueryParam("formats") String formats);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/mrToGW")
  public Response mrToGW(
      @QueryParam("pathToBaseDirectoryToWriteTo") String pathToBaseDirectoryToWriteTo,
      @QueryParam("storeName") String storeName,
      @QueryParam("indexList") String indexList, // Array of
      // Strings
      @QueryParam("visibility") String visibility,
      @QueryParam("jobTrackerHostPort") String jobTrackerHostPort,
      @QueryParam("resourceManger") String resourceManger,
      @QueryParam("extensions") String extensions, // Array of Strings
      @QueryParam("formats") String formats);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/sparkToGW")
  public Response sparkToGW(
      @QueryParam("inputDirectory") String inputDirectory,
      @QueryParam("storeName") String storeName,
      @QueryParam("indexList") String indexList, // Array of
      // Strings
      @QueryParam("visibility") String visibility,
      @QueryParam("appName") String appName,
      @QueryParam("host") String host,
      @QueryParam("master") String master,
      @QueryParam("numExecutors") Integer numExecutors,
      @QueryParam("numCores") Integer numCores,
      @QueryParam("extensions") String extensions, // Array of Strings
      @QueryParam("formats") String formats);
}
