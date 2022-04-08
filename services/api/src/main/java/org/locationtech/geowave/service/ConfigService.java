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
@Path("/v0/config")
public interface ConfigService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/list")
  public Response list(@QueryParam("filter") String filters);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/geoserver")
  public Response configGeoServer(
      @QueryParam("GeoServerURL") String GeoServerURL,
      @QueryParam("username") String username,
      @QueryParam("pass") String pass,
      @QueryParam("workspace") String workspace,
      @QueryParam("sslSecurityProtocol") String sslSecurityProtocol,
      @QueryParam("sslTrustStorePath") String sslTrustStorePath,
      @QueryParam("sslTrustStorePassword") String sslTrustStorePassword,
      @QueryParam("sslTrustStoreType") String sslTrustStoreType,
      @QueryParam("sslTruststoreProvider") String sslTruststoreProvider,
      @QueryParam("sslTrustManagerAlgorithm") String sslTrustManagerAlgorithm,
      @QueryParam("sslTrustManagerProvider") String sslTrustManagerProvider,
      @QueryParam("sslKeyStorePath") String sslKeyStorePath,
      @QueryParam("sslKeyStorePassword") String sslKeyStorePassword,
      @QueryParam("sslKeyStoreProvider") String sslKeyStoreProvider,
      @QueryParam("sslKeyPassword") String sslKeyPassword,
      @QueryParam("sslKeyStoreType") String sslKeyStoreType,
      @QueryParam("sslKeyManagerAlgorithm") String sslKeyManagerAlgorithm,
      @QueryParam("sslKeyManagerProvider") String sslKeyManagerProvider);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/hdfs")
  public Response configHDFS(@QueryParam("HDFSDefaultFSURL") String HDFSDefaultFSURL);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/set")
  public Response set(
      @QueryParam("name") String name,
      @QueryParam("value") String value,
      @QueryParam("password") Boolean password);
}
