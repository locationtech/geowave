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
@Path("/v0/gs")
public interface GeoServerService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cs/get")
  public Response getCoverageStore(
      @QueryParam("coverageStoreName") String coverageStoreName,
      @QueryParam("workspace") String workspace);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cv/get")
  public Response getCoverage(
      @QueryParam("cvgStore") String cvgStore,
      @QueryParam("coverageName") String coverageName,
      @QueryParam("workspace") String workspace);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ds/get")
  public Response getDataStore(
      @QueryParam("datastoreName") String datastoreName,
      @QueryParam("workspace") String workspace);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/fl/get")
  public Response getFeatureLayer(@QueryParam("layerName") String layerName);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/sa/get")
  public Response getStoreAdapters(@QueryParam("storeName") String storeName);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/style/get")
  public Response getStyle(@QueryParam("styleName") String styleName);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cs/list")
  public Response listCoverageStores(@QueryParam("workspace") String workspace);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cv/list")
  public Response listCoverages(
      @QueryParam("coverageStoreName") String coverageStoreName,
      @QueryParam("workspace") String workspace);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ds/list")
  public Response listDataStores(@QueryParam("workspace") String workspace);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/fl/list")
  public Response listFeatureLayers(
      @QueryParam("workspace") String workspace,
      @QueryParam("datastore") String datastore,
      @QueryParam("geowaveOnly") Boolean geowaveOnly);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/style/list")
  public Response listStyles();

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ws/list")
  public Response listWorkspaces();

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cs/add")
  public Response addCoverageStore(
      @QueryParam("GeoWaveStoreName") String geoWaveStoreName,
      @QueryParam("workspace") String workspace,
      @QueryParam("equalizerHistogramOverride") Boolean equalizerHistogramOverride,
      @QueryParam("interpolationOverride") String interpolationOverride,
      @QueryParam("scaleTo8Bit") Boolean scaleTo8Bit);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cv/add")
  public Response addCoverage(
      @QueryParam("cvgstore") String cvgstore,
      @QueryParam("coverageName") String coverageName,
      @QueryParam("workspace") String workspace);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ds/add")
  public Response addDataStore(
      @QueryParam("GeoWaveStoreName") String geoWaveStoreName,
      @QueryParam("workspace") String workspace,
      @QueryParam("datastore") String datastore);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/fl/add")
  public Response addFeatureLayer(
      @QueryParam("datastore") String datastore,
      @QueryParam("layerName") String layerName,
      @QueryParam("workspace") String workspace);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/layer/add")
  public Response addLayer(
      @QueryParam("GeoWaveStoreName") String geoWaveStoreName,
      @QueryParam("workspace") String workspace,
      @QueryParam("addOption") String addOption,
      @QueryParam("adapterId") String adapterId,
      @QueryParam("style") String style);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/style/add")
  public Response addStyle(
      @QueryParam("stylesld") String stylesld,
      @QueryParam("GeoWaveStyleName") String geoWaveStyleName);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ws/add")
  public Response addWorkspace(@QueryParam("workspaceName") String workspaceName);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cs/rm")
  public Response removeCoverageStore(
      @QueryParam("coverageStoreName") String coverageStoreName,
      @QueryParam("workspace") String workspace);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cv/rm")
  public Response removeCoverage(
      @QueryParam("cvgstore") String cvgstore,
      @QueryParam("coverageName") String coverageName,
      @QueryParam("workspace") String workspace);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ds/rm")
  public Response removeDataStore(
      @QueryParam("datastoreName") String datastoreName,
      @QueryParam("workspace") String workspace);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/fl/rm")
  public Response removeFeatureLayer(@QueryParam("layerName") String layerName);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/style/rm")
  public Response removeStyle(@QueryParam("styleName") String styleName);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ws/rm")
  public Response removeWorkspace(@QueryParam("workspaceName") String workspaceName);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/style/set")
  public Response setLayerStyle(
      @QueryParam("styleName") String styleName,
      @QueryParam("layerName") String layerName);
}
