/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service;

import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/index")
public interface IndexService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/listplugins")
  public Response listPlugins();

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/list")
  public Response listIndices(@QueryParam("storeName") final String storeName);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/spatial")
  public Response addSpatialIndex(
      @QueryParam("storeName") String storeName,
      @QueryParam("indexName") String indexName,
      @QueryParam("makeDefault") Boolean makeDefault,
      @QueryParam("numPartitions") Integer numPartitions,
      @QueryParam("partitionStrategy") String partitionStrategy,
      @QueryParam("storeTime") Boolean storeTime,
      @QueryParam("crs") String crs);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/spatial_temporal")
  public Response addSpatialTemporalIndex(
      @QueryParam("storeName") String storeName,
      @QueryParam("indexName") String indexName,
      @QueryParam("makeDefault") Boolean makeDefault,
      @QueryParam("numPartitions") Integer numPartitions,
      @QueryParam("partitionStrategy") String partitionStrategy,
      @QueryParam("periodicity") String periodicity,
      @QueryParam("bias") String bias,
      @QueryParam("maxDuplicates") Long maxDuplicates,
      @QueryParam("crs") String crs);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/add/{type}")
  public Response addIndexReRoute(
      @QueryParam("storeName") String storeName,
      @QueryParam("indexName") String indexName,
      @PathParam("type") String type,
      Map<String, String> additionalQueryParams);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/rm")
  public Response removeIndex(
      @QueryParam("storeName") String storeName,
      @QueryParam("indexName") String indexName);
}
