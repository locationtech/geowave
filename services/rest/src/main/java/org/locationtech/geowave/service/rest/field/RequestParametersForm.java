/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.field;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

public class RequestParametersForm extends RequestParameters {

  /** @param form query or form parameters; only the first value of each is used */
  public RequestParametersForm(final MultivaluedMap<String, String> form) {
    super();
    for (final String key : form.keySet()) {
      keyValuePairs.put(key, form.getFirst(key));
    }
  }

  /**
   * Parses an application/x-www-form-urlencoded body, as UTF-8.
   *
   * @throws IllegalArgumentException if the body is not validly encoded
   */
  public static RequestParametersForm fromUrlEncoded(final String body) {
    final MultivaluedMap<String, String> form = new MultivaluedHashMap<>();
    for (final String pair : body.split("&")) {
      if (!pair.isEmpty()) {
        final int equals = pair.indexOf('=');
        final String name = (equals < 0) ? pair : pair.substring(0, equals);
        final String value = (equals < 0) ? "" : pair.substring(equals + 1);
        form.add(
            URLDecoder.decode(name, StandardCharsets.UTF_8),
            URLDecoder.decode(value, StandardCharsets.UTF_8));
      }
    }
    return new RequestParametersForm(form);
  }

  @Override
  public String getString(final String parameter) {
    return (String) getValue(parameter);
  }

  @Override
  public List<?> getList(final String parameter) {
    final String[] str = splitStringParameter(parameter);
    if (str == null) {
      return null;
    }
    return Arrays.asList(str);
  }

  @Override
  public Object[] getArray(final String parameter) {
    return splitStringParameter(parameter);
  }
}
