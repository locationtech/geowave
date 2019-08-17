/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/stat")
public interface StatService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/list")
  public Response listStats(
      @QueryParam("storeName") String storeName,
      @QueryParam("typeName") String typeName,
      @QueryParam("authorizations") String authorizations,
      @QueryParam("jsonFormatFlag") Boolean jsonFormatFlag);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/calc")
  public Response calcStat(
      @QueryParam("storeName") String storeName,
      @QueryParam("datatypeName") String typeName,
      @QueryParam("statType") String statType,
      @QueryParam("authorizations") String authorizations,
      @QueryParam("jsonFormatFlag") Boolean jsonFormatFlag);

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
      @QueryParam("typeName") String typeName,
      @QueryParam("authorizations") String authorizations,
      @QueryParam("jsonFormatFlag") Boolean jsonFormatFlag);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/rm")
  public Response removeStat(
      @QueryParam("storeName") String storeName,
      @QueryParam("datatypeName") String typeName,
      @QueryParam("statType") String statType,
      @QueryParam("authorizations") String authorizations,
      @QueryParam("jsonFormatFlag") Boolean jsonFormatFlag);
}
