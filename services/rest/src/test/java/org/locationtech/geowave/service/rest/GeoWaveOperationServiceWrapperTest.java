/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.util.concurrent.TimeUnit;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import org.junit.After;
import org.junit.Test;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import com.fasterxml.jackson.databind.JsonNode;

public class GeoWaveOperationServiceWrapperTest {
  private final AsyncOperations asyncOperations = new AsyncOperations();

  @After
  public void tearDown() {
    asyncOperations.shutdown();
  }

  private ServiceEnabledCommand mockedOperation(
      final HttpMethod method,
      final Boolean successStatusIs200) throws Exception {
    return mockedOperation(method, successStatusIs200, false);
  }

  private ServiceEnabledCommand mockedOperation(
      final HttpMethod method,
      final Boolean successStatusIs200,
      final boolean isAsync) throws Exception {
    final ServiceEnabledCommand operation = Mockito.mock(ServiceEnabledCommand.class);

    Mockito.when(operation.getMethod()).thenReturn(method);
    Mockito.when(operation.runAsync()).thenReturn(isAsync);
    Mockito.when(operation.successStatusIs200()).thenReturn(successStatusIs200);
    Mockito.when(operation.computeResults(ArgumentMatchers.any())).thenReturn("result");

    return operation;
  }

  private Response handle(
      final ServiceEnabledCommand operation,
      final HttpMethod method,
      final MediaType contentType,
      final String body) {
    return new GeoWaveOperationServiceWrapper<>(operation, null, asyncOperations).handle(
        method,
        contentType,
        body,
        new MultivaluedHashMap<>());
  }

  private static JsonNode json(final Response response) throws Exception {
    return JsonResponses.MAPPER.readTree((String) response.getEntity());
  }

  @Test
  public void getMethodReturnsSuccessStatus() throws Exception {
    final Response response =
        handle(mockedOperation(HttpMethod.GET, true), HttpMethod.GET, null, null);
    assertEquals(200, response.getStatus());
    assertEquals("COMPLETE", json(response).get("status").asText());
    assertEquals("result", json(response).get("data").asText());
  }

  @Test
  public void postMethodReturnsSuccessStatus() throws Exception {
    final Response response =
        handle(
            mockedOperation(HttpMethod.POST, false),
            HttpMethod.POST,
            MediaType.APPLICATION_JSON_TYPE,
            "{}");
    assertEquals(201, response.getStatus());
  }

  @Test
  public void postWithFormBodyReturnsSuccessStatus() throws Exception {
    final Response response =
        handle(
            mockedOperation(HttpMethod.POST, true),
            HttpMethod.POST,
            MediaType.APPLICATION_FORM_URLENCODED_TYPE,
            "a=b");
    assertEquals(200, response.getStatus());
  }

  @Test
  public void wrongMethodIsNotAllowed() throws Exception {
    final Response response =
        handle(mockedOperation(HttpMethod.POST, false), HttpMethod.GET, null, null);
    assertEquals(405, response.getStatus());
    assertEquals("POST", response.getHeaderString("Allow"));
    assertNull(response.getEntity());
  }

  @Test
  public void asyncMethodReturnsSuccessStatus() throws Exception {
    final Response response =
        handle(mockedOperation(HttpMethod.POST, true, true), HttpMethod.POST, null, null);
    assertEquals(200, response.getStatus());
    final JsonNode json = json(response);
    assertEquals("STARTED", json.get("status").asText());
    final String id = json.get("data").asText();
    assertNotNull(asyncOperations.get(id));
    assertEquals("result", asyncOperations.get(id).get(10, TimeUnit.SECONDS));
  }

  @Test
  public void failureReturnsServerError() throws Exception {
    final ServiceEnabledCommand operation = mockedOperation(HttpMethod.GET, true);
    Mockito.when(operation.computeResults(ArgumentMatchers.any())).thenThrow(
        new IllegalStateException("boom"));
    final Response response = handle(operation, HttpMethod.GET, null, null);
    assertEquals(500, response.getStatus());
    final JsonNode json = json(response);
    assertEquals("ERROR", json.get("status").asText());
    assertEquals("boom", json.get("data").get("message").asText());
  }
}
