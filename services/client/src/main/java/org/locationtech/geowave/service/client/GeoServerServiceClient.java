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
import org.locationtech.geowave.service.GeoServerService;

public class GeoServerServiceClient {
  private final GeoServerService geoServerService;

  public GeoServerServiceClient(final String baseUrl) {
    this(baseUrl, null, null);
  }

  public GeoServerServiceClient(final String baseUrl, final String user, final String password) {
    // ClientBuilder bldr = ClientBuilder.newBuilder();
    // if (user != null && password != null) {
    // HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
    // user,
    // password);
    // bldr.register(feature);
    // }
    geoServerService =
        WebResourceFactory.newResource(
            GeoServerService.class,
            ClientBuilder.newClient().register(MultiPartFeature.class).target(baseUrl));
  }

  public Response getCoverageStore(final String coverageStoreName, final String workspace) {

    final Response resp = geoServerService.getCoverageStore(coverageStoreName, workspace);
    return resp;
  }

  public Response getCoverageStore(final String coverageStoreName) {
    return getCoverageStore(coverageStoreName, null);
  }

  public Response getCoverage(
      final String cvgstore,
      final String coverageName,
      final String workspace) {

    final Response resp = geoServerService.getCoverage(cvgstore, coverageName, workspace);
    return resp;
  }

  public Response getCoverage(final String cvgstore, final String coverageName) {
    return getCoverage(cvgstore, coverageName, null);
  }

  public Response getDataStore(final String datastoreName, final String workspace) {
    final Response resp = geoServerService.getDataStore(datastoreName, workspace);
    return resp;
  }

  public Response getDataStore(final String datastoreName) {
    return getDataStore(datastoreName, null);
  }

  public Response getFeatureLayer(final String layerName) {

    final Response resp = geoServerService.getFeatureLayer(layerName);
    return resp;
  }

  public Response getStoreAdapters(final String storeName) {

    final Response resp = geoServerService.getStoreAdapters(storeName);
    return resp;
  }

  public Response getStyle(final String styleName) {

    final Response resp = geoServerService.getStyle(styleName);
    return resp;
  }

  public Response listCoverageStores(final String workspace) {

    final Response resp = geoServerService.listCoverageStores(workspace);
    return resp;
  }

  public Response listCoverageStores() {
    return listCoverageStores(null);
  }

  public Response listCoverages(final String coverageStoreName, final String workspace) {
    final Response resp = geoServerService.listCoverages(coverageStoreName, workspace);
    return resp;
  }

  public Response listCoverages(final String coverageStoreName) {
    return listCoverages(coverageStoreName, null);
  }

  public Response listDataStores(final String workspace) {
    final Response resp = geoServerService.listDataStores(workspace);
    return resp;
  }

  public Response listDataStores() {
    return listDataStores(null);
  }

  public Response listFeatureLayers(
      final String workspace,
      final String datastore,
      final Boolean geowaveOnly) {

    final Response resp = geoServerService.listFeatureLayers(workspace, datastore, geowaveOnly);
    return resp;
  }

  public Response listFeatureLayers() {
    return listFeatureLayers(null, null, null);
  }

  public Response listStyles() {
    return geoServerService.listStyles();
  }

  public Response listWorkspaces() {
    return geoServerService.listWorkspaces();
  }

  // POST Requests
  public Response addCoverageStore(
      final String geoWaveStoreName,
      final String workspace,
      final Boolean equalizerHistogramOverride,
      final String interpolationOverride,
      final Boolean scaleTo8Bit) {

    final Response resp =
        geoServerService.addCoverageStore(
            geoWaveStoreName,
            workspace,
            equalizerHistogramOverride,
            interpolationOverride,
            scaleTo8Bit);
    return resp;
  }

  public Response addCoverageStore(final String GeoWaveStoreName) {
    return addCoverageStore(GeoWaveStoreName, null, null, null, null);
  }

  public Response addCoverage(
      final String cvgstore,
      final String coverageName,
      final String workspace) {

    final Response resp = geoServerService.addCoverage(cvgstore, coverageName, workspace);
    return resp;
  }

  public Response addCoverage(final String cvgstore, final String coverageName) {
    return addCoverage(cvgstore, coverageName, null);
  }

  public Response addDataStore(
      final String geoWaveStoreName,
      final String workspace,
      final String datastore) {

    final Response resp = geoServerService.addDataStore(geoWaveStoreName, workspace, datastore);
    return resp;
  }

  public Response addDataStore(final String geoWaveStoreName) {
    return addDataStore(geoWaveStoreName, null, null);
  }

  public Response addFeatureLayer(
      final String datastore,
      final String layerName,
      final String workspace) {

    final Response resp = geoServerService.addFeatureLayer(datastore, layerName, workspace);
    return resp;
  }

  public Response addFeatureLayer(final String datastore, final String layerName) {
    return addFeatureLayer(datastore, layerName, null);
  }

  public Response addLayer(
      final String geoWaveStoreName,
      final String workspace,
      final String addOption,
      final String adapterId,
      final String style) {

    final Response resp =
        geoServerService.addLayer(geoWaveStoreName, workspace, addOption, adapterId, style);
    return resp;
  }

  public Response addLayer(final String geoWaveStoreName) {
    return addLayer(geoWaveStoreName, null, null, null, null);
  }

  public Response addStyle(final String stylesld, final String geoWaveStyleName) {

    final Response resp = geoServerService.addStyle(stylesld, geoWaveStyleName);
    return resp;
  }

  public Response addWorkspace(final String workspaceName) {

    final Response resp = geoServerService.addWorkspace(workspaceName);
    return resp;
  }

  public Response removeCoverageStore(final String coverageStoreName, final String workspace) {

    final Response resp = geoServerService.removeCoverageStore(coverageStoreName, workspace);
    return resp;
  }

  public Response removeCoverageStore(final String coverageStoreName) {
    return removeCoverageStore(coverageStoreName, null);
  }

  public Response removeCoverage(
      final String cvgstore,
      final String coverageName,
      final String workspace) {

    final Response resp = geoServerService.removeCoverage(cvgstore, coverageName, workspace);
    return resp;
  }

  public Response removeCoverage(final String cvgstore, final String coverageName) {
    return removeCoverage(cvgstore, coverageName, null);
  }

  public Response removeDataStore(final String datastoreName, final String workspace) {

    final Response resp = geoServerService.removeDataStore(datastoreName, workspace);
    return resp;
  }

  public Response removeDataStore(final String datastoreName) {
    return removeDataStore(datastoreName, null);
  }

  public Response removeFeatureLayer(final String layerName) {

    final Response resp = geoServerService.removeFeatureLayer(layerName);
    return resp;
  }

  public Response removeStyle(final String styleName) {

    final Response resp = geoServerService.removeStyle(styleName);
    return resp;
  }

  public Response removeWorkspace(final String workspaceName) {

    final Response resp = geoServerService.removeWorkspace(workspaceName);
    return resp;
  }

  public Response setLayerStyle(final String styleName, final String layerName) {

    final Response resp = geoServerService.setLayerStyle(styleName, layerName);
    return resp;
  }
}
