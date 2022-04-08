/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.Map;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.store.api.RowBuilder;
import com.beust.jcommander.internal.Maps;

public class MapRowBuilder implements RowBuilder<Map<String, Object>> {

  private final Map<String, Object> sourceMap;

  public MapRowBuilder() {
    sourceMap = Maps.newHashMap();
  }

  public MapRowBuilder(final Map<String, Object> sourceMap) {
    this.sourceMap = sourceMap;
  }

  @Override
  public void setField(String fieldName, Object fieldValue) {
    sourceMap.put(fieldName, fieldValue);
  }

  @Override
  public void setFields(Map<String, Object> values) {
    sourceMap.putAll(values);
  }

  @Override
  public Map<String, Object> buildRow(byte[] dataId) {
    final Map<String, Object> returnValue =
        sourceMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    sourceMap.clear();
    return returnValue;
  }

}
