/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.field;

import java.lang.reflect.Field;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class ListMainParam extends AbstractMainParam<List> {

  public ListMainParam(
      int ordinal,
      int totalMainParams,
      Field listMainParamField,
      RestField<List> delegateField,
      Object instance) {
    super(ordinal, totalMainParams, listMainParamField, delegateField, instance);
  }

  @Override
  protected String valueToString(List value) {
    return StringUtils.join(value, ',');
  }
}
