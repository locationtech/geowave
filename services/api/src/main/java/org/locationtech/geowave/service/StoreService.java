/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service;

import java.util.Map;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/store")
public interface StoreService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/listplugins")
  public Response listPlugins();

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/version")
  public Response version(@QueryParam("storeName") String storeName);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/clear")
  public Response clear(@QueryParam("storeName") String storeName);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/hbase")
  public Response addHBaseStore(
      @QueryParam("name") String name,
      @QueryParam("zookeeper") String zookeeper,
      @QueryParam("makeDefault") Boolean makeDefault,
      @QueryParam("geowaveNamespace") String geowaveNamespace,
      @QueryParam("disableServiceSide") Boolean disableServiceSide,
      @QueryParam("coprocessorjar") String coprocessorjar,
      @QueryParam("persistAdapter") Boolean persistAdapter,
      @QueryParam("persistIndex") Boolean persistIndex,
      @QueryParam("persistDataStatistics") Boolean persistDataStatistics,
      @QueryParam("createTable") Boolean createTable,
      @QueryParam("useAltIndex") Boolean useAltIndex,
      @QueryParam("enableBlockCache") Boolean enableBlockCache);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/accumulo")
  public Response addAccumuloStore(
      @QueryParam("name") String name,
      @QueryParam("zookeeper") String zookeeper,
      @QueryParam("instance") String instance,
      @QueryParam("user") String user,
      @QueryParam("password") String password,
      @QueryParam("makeDefault") Boolean makeDefault,
      @QueryParam("geowaveNamespace") String geowaveNamespace,
      @QueryParam("useLocalityGroups") Boolean useLocalityGroups,
      @QueryParam("persistAdapter") Boolean persistAdapter,
      @QueryParam("persistIndex") Boolean persistIndex,
      @QueryParam("persistDataStatistics") Boolean persistDataStatistics,
      @QueryParam("createTable") Boolean createTable,
      @QueryParam("useAltIndex") Boolean useAltIndex,
      @QueryParam("enableBlockCache") Boolean enableBlockCache);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/bigtable")
  public Response addBigTableStore(
      @QueryParam("name") String name,
      @QueryParam("makeDefault") Boolean makeDefault,
      @QueryParam("scanCacheSize") Integer scanCacheSize,
      @QueryParam("projectId") String projectId,
      @QueryParam("instanceId") String instanceId,
      @QueryParam("geowaveNamespace") String geowaveNamespace,
      @QueryParam("useLocalityGroups") Boolean useLocalityGroups,
      @QueryParam("persistAdapter") Boolean persistAdapter,
      @QueryParam("persistIndex") Boolean persistIndex,
      @QueryParam("persistDataStatistics") Boolean persistDataStatistics,
      @QueryParam("createTable") Boolean createTable,
      @QueryParam("useAltIndex") Boolean useAltIndex,
      @QueryParam("enableBlockCache") Boolean enableBlockCache);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/dynamodb")
  public Response addDynamoDBStore(
      @QueryParam("name") String name,
      @QueryParam("makeDefault") Boolean makeDefault,
      @QueryParam("endpoint") String endpoint,
      @QueryParam("region") String region,
      @QueryParam("writeCapacity") Long writeCapacity,
      @QueryParam("readCapacity") Long readCapacity,
      @QueryParam("maxConnections") Integer maxConnections,
      @QueryParam("protocol") String protocol,
      @QueryParam("enableCacheResponseMetadata") Boolean enableCacheResponseMetadata,
      @QueryParam("gwNamespace") String gwNamespace,
      @QueryParam("persistAdapter") Boolean persistAdapter,
      @QueryParam("persistIndex") Boolean persistIndex,
      @QueryParam("persistDataStatistics") Boolean persistDataStatistics,
      @QueryParam("createTable") Boolean createTable,
      @QueryParam("useAltIndex") Boolean useAltIndex,
      @QueryParam("enableBlockCache") Boolean enableBlockCache,
      @QueryParam("enableServerSideLibrary") Boolean enableServerSideLibrary);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/cassandra")
  public Response addCassandraStore(
      @QueryParam("name") String name,
      @QueryParam("makeDefault") Boolean makeDefault,
      @QueryParam("contactPoint") String contactPoint,
      @QueryParam("batchWriteSize") Integer batchWriteSize,
      @QueryParam("durableWrites") Boolean durableWrites,
      @QueryParam("replicationFactor") Integer replicationFactor,
      @QueryParam("geowaveNamespace") String geowaveNamespace,
      @QueryParam("persistAdapter") Boolean persistAdapter,
      @QueryParam("persistIndex") Boolean persistIndex,
      @QueryParam("persistDataStatistics") Boolean persistDataStatistics,
      @QueryParam("createTable") Boolean createTable,
      @QueryParam("useAltIndex") Boolean useAltIndex,
      @QueryParam("enableBlockCache") Boolean enableBlockCache,
      @QueryParam("enableServerSideLibrary") Boolean enableServerSideLibrary);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/{type}")
  public Response addStoreReRoute(
      @QueryParam("name") String name,
      @PathParam("type") String type,
      @QueryParam("geowaveNamespace") @DefaultValue("") String geowaveNamespace,
      Map<String, String> additionalQueryParams);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/rm")
  public Response removeStore(@QueryParam("name") String name);
}
