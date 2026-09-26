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
import org.junit.Test;
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

  @Test
  public void instantiationSuccessfulWithJson() throws Exception {
    classUnderTest = new RequestParametersJson("{}");
  }

  @Test
  public void getValueReturnsJsonString() throws Exception {
    testJSON = MAPPER.createObjectNode();
    testJSON.put(testKey, testString);
    classUnderTest = new RequestParametersJson(testJSON.toString());

    assertEquals(testString, classUnderTest.getValue(testKey));
  }

  @Test
  public void getStringReturnsJsonString() throws Exception {
    testJSON = MAPPER.createObjectNode();

    testJSON.put(testKey, testString);
    classUnderTest = new RequestParametersJson(testJSON.toString());

    assertEquals(testString, classUnderTest.getString(testKey));
  }

  @Test
  public void getListReturnsJsonList() throws Exception {
    testJSON = MAPPER.createObjectNode();

    testJSON.set(testKey, MAPPER.valueToTree(testList));
    classUnderTest = new RequestParametersJson(testJSON.toString());

    assertEquals(testList, classUnderTest.getList(testKey));
  }

  @Test
  public void getArrayReturnsJsonArray() throws Exception {
    testJSON = MAPPER.createObjectNode();

    testJSON.set(testKey, MAPPER.valueToTree(testArray));
    classUnderTest = new RequestParametersJson(testJSON.toString());

    assertArrayEquals(testArray, classUnderTest.getArray(testKey));
  }

  @Test
  public void getValueReturnsJsonNumber() throws Exception {
    testJSON = MAPPER.createObjectNode();

    testJSON.put(testKey, testNumber);
    classUnderTest = new RequestParametersJson(testJSON.toString());

    assertEquals(testNumber, classUnderTest.getValue(testKey));
  }

  @Test(expected = IOException.class)
  public void malformedJsonIsRejected() throws Exception {
    new RequestParametersJson("{not json");
  }
}
