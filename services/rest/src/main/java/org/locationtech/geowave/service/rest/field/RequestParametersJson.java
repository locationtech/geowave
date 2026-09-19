/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.field;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.restlet.representation.Representation;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RequestParametersJson extends RequestParameters {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public RequestParametersJson(final Representation request) throws IOException {
    super();
    injectJsonParams(request.getText());
  }

  @Override
  public String getString(final String parameter) {
    return (String) getValue(parameter);
  }

  @Override
  public List<?> getList(final String parameter) {
    return (List<?>) getValue(parameter);
  }

  @Override
  public Object[] getArray(final String parameter) {
    final List<?> list = getList(parameter);
    return (list == null) ? null : list.toArray();
  }

  /**
   * Jackson maps a JSON document onto plain Java types -- String, Integer, Double, Boolean, List
   * and Map -- which is exactly what {@link RequestParameters#getValue} is expected to hand back.
   */
  private void injectJsonParams(final String jsonString) throws IOException {
    keyValuePairs.putAll(MAPPER.readValue(jsonString, new TypeReference<Map<String, Object>>() {}));
  }
}
