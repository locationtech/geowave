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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.restlet.data.Form;
import org.restlet.data.Parameter;

public class RequestParametersFormTest {

  private RequestParametersForm classUnderTest;

  private final String testKey = "foo";
  private final String testString = "bar";
  private final List<String> testList = new ArrayList<>(Arrays.asList("bar", "baz"));
  private final String[] testArray = {"foo", "bar"};

  private Form mockedForm(final Map<String, String> inputKeyValuePairs) {
    final String keyName;
    final Form form = Mockito.mock(Form.class);
    Mockito.when(form.getNames()).thenReturn(inputKeyValuePairs.keySet());
    Mockito.when(form.getFirst(Matchers.anyString())).thenAnswer(
        i -> mockedFormParameter(inputKeyValuePairs.get(i.getArguments()[0])));

    return form;
  }

  private Parameter mockedFormParameter(final String value) {
    final Parameter param = Mockito.mock(Parameter.class);

    Mockito.when(param.getValue()).thenReturn(value);

    return param;
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void instantiationSuccessfulWithForm() throws Exception {
    final Map<String, String> testKVP = new HashMap<>();

    final Form form = mockedForm(testKVP);

    classUnderTest = new RequestParametersForm(form);
  }

  @Test
  public void getStringReturnsFormString() throws Exception {
    final Map<String, String> testKVP = new HashMap<>();

    final Form form = mockedForm(testKVP);
    testKVP.put(testKey, testString);

    classUnderTest = new RequestParametersForm(form);

    assertEquals(testString, classUnderTest.getString(testKey));
  }

  @Test
  public void getListReturnsFormList() throws Exception {
    final Map<String, String> testKVP = new HashMap<>();

    final String testJoinedString = String.join(",", testList);
    final Form form = mockedForm(testKVP);
    testKVP.put(testKey, testJoinedString);

    classUnderTest = new RequestParametersForm(form);

    assertEquals(testList, classUnderTest.getList(testKey));
  }

  @Test
  public void getArrayReturnsFormArray() throws Exception {
    final Map<String, String> testKVP = new HashMap<>();

    final String testJoinedString = String.join(",", testArray);
    final Form form = mockedForm(testKVP);
    testKVP.put(testKey, testJoinedString);

    classUnderTest = new RequestParametersForm(form);

    assertArrayEquals(testArray, classUnderTest.getArray(testKey));
  }
}
