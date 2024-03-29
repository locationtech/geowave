/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.field;

import java.util.Arrays;
import java.util.List;
import org.restlet.data.Form;

public class RequestParametersForm extends RequestParameters {

  public RequestParametersForm(final Form form) {
    super();
    for (final String key : form.getNames()) {
      // For each parameter in the form, add the parameter name and value
      // to the Map<String, Object>.
      keyValuePairs.put(key, form.getFirst(key).getValue());
    }
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
