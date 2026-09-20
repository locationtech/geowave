/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.field;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RequestParametersJsonTest {

  private RequestParametersJson classUnderTest;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ObjectNode testJSON;

  private final int testNumber = 42;
  private final String testKey = "foo";
  private final String testString = "bar";
  private final List<String> testList = new ArrayList<>(Arrays.asList("bar", "baz"));
  private final String[] testArray = {"foo", "bar"};

  private Representation mockedJsonRequest(final String jsonString) throws IOException {
    final Representation request = mockedRequest(MediaType.APPLICATION_JSON);

    Mockito.when(request.getText()).thenReturn(jsonString);

    return request;
  }

  private Representation mockedRequest(final MediaType mediaType) {

    final Representation request = Mockito.mock(Representation.class);

    Mockito.when(request.getMediaType()).thenReturn(mediaType);

    return request;
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void instantiationSuccessfulWithJson() throws Exception {
    final Representation request = mockedJsonRequest("{}");

    classUnderTest = new RequestParametersJson(request);
  }

  @Test
  public void getValueReturnsJsonString() throws Exception {
    testJSON = MAPPER.createObjectNode();
    testJSON.put(testKey, testString);
    final Representation request = mockedJsonRequest(testJSON.toString());
    classUnderTest = new RequestParametersJson(request);

    assertEquals(testString, classUnderTest.getValue(testKey));
  }

  @Test
  public void getStringReturnsJsonString() throws Exception {
    testJSON = MAPPER.createObjectNode();

    testJSON.put(testKey, testString);
    final Representation request = mockedJsonRequest(testJSON.toString());
    classUnderTest = new RequestParametersJson(request);

    assertEquals(testString, classUnderTest.getString(testKey));
  }

  @Test
  public void getListReturnsJsonList() throws Exception {
    testJSON = MAPPER.createObjectNode();

    testJSON.set(testKey, MAPPER.valueToTree(testList));
    final Representation request = mockedJsonRequest(testJSON.toString());
    classUnderTest = new RequestParametersJson(request);

    assertEquals(testList, classUnderTest.getList(testKey));
  }

  @Test
  public void getArrayReturnsJsonArray() throws Exception {
    testJSON = MAPPER.createObjectNode();

    testJSON.set(testKey, MAPPER.valueToTree(testArray));
    final Representation request = mockedJsonRequest(testJSON.toString());
    classUnderTest = new RequestParametersJson(request);

    assertArrayEquals(testArray, classUnderTest.getArray(testKey));
  }

  @Test
  public void getValueReturnsJsonNumber() throws Exception {
    testJSON = MAPPER.createObjectNode();

    testJSON.put(testKey, testNumber);
    final Representation request = mockedJsonRequest(testJSON.toString());
    classUnderTest = new RequestParametersJson(request);

    assertEquals(testNumber, classUnderTest.getValue(testKey));
  }
}
