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
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.representation.Representation;

public class RequestParametersJson extends RequestParameters {

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
    return jsonArrayToList((JSONArray) getValue(parameter));
  }

  @Override
  public Object[] getArray(final String parameter) {
    return jsonArrayToArray((JSONArray) getValue(parameter));
  }

  private void injectJsonParams(final String jsonString) {
    final JSONObject json = new JSONObject(jsonString);
    for (final String key : json.keySet()) {
      // For each parameter in the form, add the parameter name and value
      // to the Map<String, Object>.
      try {
        // First try to add the value as a JSONArray.
        keyValuePairs.put(key, json.getJSONArray(key));
      } catch (final JSONException e) {
        // If that does not work, add the parameter as an Object.
        keyValuePairs.put(key, json.get(key));
      }
    }
  }

  private Object[] jsonArrayToArray(final JSONArray jsonArray) {
    if (jsonArray == null) {
      return null;
    }

    // Initialize the output Array.
    final int jsonArrayLenth = jsonArray.length();
    final Object[] outArray = new Object[jsonArrayLenth];
    for (int i = 0; i < jsonArrayLenth; i++) {
      // Then add each JSONArray element to it.
      outArray[i] = jsonArray.get(i);
    }
    return outArray;
  }

  private List<Object> jsonArrayToList(final JSONArray jsonArray) {
    if (jsonArray == null) {
      return null;
    }

    // Initialize the output List.
    final int jsonArrayLenth = jsonArray.length();
    final List<Object> outList = new ArrayList<>();
    for (int i = 0; i < jsonArrayLenth; i++) {
      // Then add each JSONArray element to it.
      outList.add(jsonArray.get(i));
    }
    return outList;
  }
}
