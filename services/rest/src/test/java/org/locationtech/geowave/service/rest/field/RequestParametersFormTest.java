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
import static org.junit.Assert.assertNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.junit.Test;

public class RequestParametersFormTest {

  private RequestParametersForm classUnderTest;

  private final String testKey = "foo";
  private final String testString = "bar";
  private final List<String> testList = new ArrayList<>(Arrays.asList("bar", "baz"));
  private final String[] testArray = {"foo", "bar"};

  private static MultivaluedMap<String, String> form(final String key, final String... values) {
    final MultivaluedMap<String, String> form = new MultivaluedHashMap<>();
    form.addAll(key, values);
    return form;
  }

  @Test
  public void instantiationSuccessfulWithForm() throws Exception {
    classUnderTest = new RequestParametersForm(new MultivaluedHashMap<>());
    assertNull(classUnderTest.getString(testKey));
  }

  @Test
  public void getStringReturnsFormString() throws Exception {
    classUnderTest = new RequestParametersForm(form(testKey, testString));

    assertEquals(testString, classUnderTest.getString(testKey));
  }

  @Test
  public void getStringReturnsFirstValue() throws Exception {
    classUnderTest = new RequestParametersForm(form(testKey, testString, "other"));

    assertEquals(testString, classUnderTest.getString(testKey));
  }

  @Test
  public void getListReturnsFormList() throws Exception {
    classUnderTest = new RequestParametersForm(form(testKey, String.join(",", testList)));

    assertEquals(testList, classUnderTest.getList(testKey));
  }

  @Test
  public void getArrayReturnsFormArray() throws Exception {
    classUnderTest = new RequestParametersForm(form(testKey, String.join(",", testArray)));

    assertArrayEquals(testArray, classUnderTest.getArray(testKey));
  }

  @Test
  public void urlEncodedBodyIsDecoded() throws Exception {
    classUnderTest = RequestParametersForm.fromUrlEncoded("foo=a%2Cb+c&empty=&flag&foo=ignored");

    assertEquals("a,b c", classUnderTest.getString("foo"));
    assertEquals(Arrays.asList("a", "b c"), classUnderTest.getList("foo"));
    assertEquals("", classUnderTest.getString("empty"));
    assertEquals("", classUnderTest.getString("flag"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void malformedUrlEncodedBodyIsRejected() throws Exception {
    RequestParametersForm.fromUrlEncoded("foo=%zz");
  }
}
