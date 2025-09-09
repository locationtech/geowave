/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/stat")
public interface StatService {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/list")
  public Response listStats(
      @QueryParam("storeName") String storeName,
      @QueryParam("indexName") String indexName,
      @QueryParam("typeName") String typeName,
      @QueryParam("fieldName") String fieldName,
      @QueryParam("tag") String tag,
      @QueryParam("authorizations") String authorizations,
      @QueryParam("limit") Integer limit,
      @QueryParam("csv") Boolean csv);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/compact")
  public Response combineStats(@QueryParam("store_name") String store_name);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/recalc")
  public Response recalcStats(
      @QueryParam("storeName") String storeName,
      @QueryParam("statType") String statType,
      @QueryParam("indexName") String indexName,
      @QueryParam("typeName") String typeName,
      @QueryParam("fieldName") String fieldName,
      @QueryParam("tag") String tag,
      @QueryParam("all") Boolean allFlag,
      @QueryParam("authorizations") String authorizations);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/rm")
  public Response removeStat(
      @QueryParam("storeName") String storeName,
      @QueryParam("statType") String statType,
      @QueryParam("indexName") String indexName,
      @QueryParam("typeName") String typeName,
      @QueryParam("fieldName") String fieldName,
      @QueryParam("tag") String tag,
      @QueryParam("all") Boolean all,
      @QueryParam("force") Boolean force);
}
