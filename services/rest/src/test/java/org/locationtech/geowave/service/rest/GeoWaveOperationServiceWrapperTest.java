/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.representation.Representation;

public class GeoWaveOperationServiceWrapperTest {

  private GeoWaveOperationServiceWrapper classUnderTest;

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
    Mockito.when(operation.computeResults(Matchers.any())).thenReturn(null);

    return operation;
  }

  private Representation mockedRequest(final MediaType mediaType) throws IOException {

    final Representation request = Mockito.mock(Representation.class);

    Mockito.when(request.getMediaType()).thenReturn(mediaType);
    Mockito.when(request.getText()).thenReturn("{}");

    return request;
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void getMethodReturnsSuccessStatus() throws Exception {

    // Rarely used Teapot Code to check.
    final Boolean successStatusIs200 = true;

    final ServiceEnabledCommand operation = mockedOperation(HttpMethod.GET, successStatusIs200);

    classUnderTest = new GeoWaveOperationServiceWrapper(operation, null);
    classUnderTest.setResponse(new Response(null));
    classUnderTest.setRequest(new Request(Method.GET, "foo.bar"));
    classUnderTest.restGet();
    Assert.assertEquals(
        successStatusIs200,
        classUnderTest.getResponse().getStatus().equals(Status.SUCCESS_OK));
  }

  @Test
  public void postMethodReturnsSuccessStatus() throws Exception {

    // Rarely used Teapot Code to check.
    final Boolean successStatusIs200 = false;

    final ServiceEnabledCommand operation = mockedOperation(HttpMethod.POST, successStatusIs200);

    classUnderTest = new GeoWaveOperationServiceWrapper(operation, null);
    classUnderTest.setResponse(new Response(null));
    classUnderTest.restPost(mockedRequest(MediaType.APPLICATION_JSON));
    Assert.assertEquals(
        successStatusIs200,
        classUnderTest.getResponse().getStatus().equals(Status.SUCCESS_OK));
  }

  @Test
  @Ignore
  public void asyncMethodReturnsSuccessStatus() throws Exception {

    // Rarely used Teapot Code to check.
    final Boolean successStatusIs200 = true;

    final ServiceEnabledCommand operation =
        mockedOperation(HttpMethod.POST, successStatusIs200, true);

    classUnderTest = new GeoWaveOperationServiceWrapper(operation, null);
    classUnderTest.setResponse(new Response(null));
    classUnderTest.restPost(null);

    // TODO: Returns 500. Error Caught at
    // "final Context appContext = Application.getCurrent().getContext();"
    Assert.assertEquals(
        successStatusIs200,
        classUnderTest.getResponse().getStatus().equals(Status.SUCCESS_OK));
  }
}
