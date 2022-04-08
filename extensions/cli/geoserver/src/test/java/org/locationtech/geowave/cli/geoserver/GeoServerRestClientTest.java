/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.internal.Console;

public class GeoServerRestClientTest {
  WebTarget webTarget;
  GeoServerConfig config;
  GeoServerRestClient client;

  private WebTarget mockedWebTarget() {
    final WebTarget webTarget = Mockito.mock(WebTarget.class);
    final Invocation.Builder invBuilder = Mockito.mock(Invocation.Builder.class);
    final Response response = Mockito.mock(Response.class);

    Mockito.when(webTarget.path(Matchers.anyString())).thenReturn(webTarget);
    Mockito.when(
        webTarget.queryParam(Matchers.eq("quietOnNotFound"), Matchers.anyBoolean())).thenReturn(
            webTarget);
    Mockito.when(webTarget.request()).thenReturn(invBuilder);

    Mockito.when(invBuilder.get()).thenReturn(response);
    Mockito.when(invBuilder.delete()).thenReturn(response);
    Mockito.when(invBuilder.post(Matchers.any(Entity.class))).thenReturn(response);

    return webTarget;
  }

  @Before
  public void prepare() {
    webTarget = mockedWebTarget();
    final Console console = new JCommander().getConsole();
    config = new GeoServerConfig(console);
    client = GeoServerRestClient.getInstance(config, console);
    client.setWebTarget(webTarget);
  }

  // We want to start each test with a new instance
  @After
  public void cleanUp() {
    GeoServerRestClient.invalidateInstance();
  }

  @Test
  public void testGetFeatureLayer() {
    client.getFeatureLayer("some_layer", false);
    Mockito.verify(webTarget).path("rest/layers/some_layer.json");
  }

  @Test
  public void testGetConfig() {
    final GeoServerConfig returnedConfig = client.getConfig();
    Assert.assertEquals(config, returnedConfig);
  }

  @Test
  public void testGetCoverage() {
    client.getCoverage("some_workspace", "some_cvgStore", "some_coverage", false);
    Mockito.verify(webTarget).path(
        "rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages/some_coverage.json");
  }

  @Test
  public void testGetCoverageStores() {
    client.getCoverageStores("some_workspace");
    Mockito.verify(webTarget).path("rest/workspaces/some_workspace/coveragestores.json");
  }

  @Test
  public void testGetCoverages() {
    client.getCoverages("some_workspace", "some_cvgStore");
    Mockito.verify(webTarget).path(
        "rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages.json");
  }

  @Test
  public void testGetDatastore() {
    client.getDatastore("some_workspace", "some_datastore", false);
    Mockito.verify(webTarget).path("rest/workspaces/some_workspace/datastores/some_datastore.json");
  }

  @Test
  public void testGetStyle() {
    client.getStyle("some_style", false);
    Mockito.verify(webTarget).path("rest/styles/some_style.sld");
  }

  @Test
  public void testGetStyles() {
    client.getStyles();
    Mockito.verify(webTarget).path("rest/styles.json");
  }

  @Test
  public void testGetWorkspaces() {
    client.getWorkspaces();
    Mockito.verify(webTarget).path("rest/workspaces.json");
  }

  @Test
  public void addFeatureLayer() {
    client.addFeatureLayer("some_workspace", "some_datastore", "some_layer", "some_style");
    Mockito.verify(webTarget).path("rest/layers/some_layer.json");
  }

  @Test
  public void addCoverage() {
    client.addCoverage("some_workspace", "some_cvgStore", "some_coverage");
    Mockito.verify(webTarget).path(
        "rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages");
  }

  @Test
  public void addWorkspace() {
    client.addWorkspace("some_workspace");
    Mockito.verify(webTarget).path("rest/workspaces");
  }
}
