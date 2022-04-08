/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import com.beust.jcommander.internal.Maps;

public class JsonFieldLevelVisibilityHandlerTest {
  DataTypeAdapter<Map<String, String>> adapter;
  Object[] defaults;
  Map<String, String> entry;
  final VisibilityHandler visHandler = new JsonFieldLevelVisibilityHandler("vis");

  @Before
  public void setup() {
    // We're not really using this as a full data adapter, so we can ignore most of the methods
    adapter = new DataTypeAdapter<Map<String, String>>() {

      @Override
      public byte[] toBinary() {
        return null;
      }

      @Override
      public void fromBinary(final byte[] bytes) {}

      @Override
      public String getTypeName() {
        return null;
      }

      @Override
      public byte[] getDataId(final Map<String, String> entry) {
        return null;
      }

      @Override
      public Object getFieldValue(final Map<String, String> entry, final String fieldName) {
        return entry.get(fieldName);
      }

      @Override
      public Class<Map<String, String>> getDataClass() {
        return null;
      }

      @Override
      public RowBuilder<Map<String, String>> newRowBuilder(
          final FieldDescriptor<?>[] outputFieldDescriptors) {
        return null;
      }

      @Override
      public FieldDescriptor<?>[] getFieldDescriptors() {
        return null;
      }

      @Override
      public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
        return null;
      }

    };

    entry = Maps.newHashMap();
    entry.put("pop", "pop");
    entry.put("pid", "pid");
    entry.put("vis", "{\"pid\":\"TS\", \"geo.*\":\"S\"}");
    entry.put("geometry", "POINT(0, 0)");
  }

  @Test
  public void testPIDNonDefault() {
    assertEquals("TS", visHandler.getVisibility(adapter, entry, "pid"));
  }

  @Test
  public void testPOPNonDefault() {
    assertNull(visHandler.getVisibility(adapter, entry, "pop"));
  }

  @Test
  public void testGEORegexDefault() {
    assertEquals("S", visHandler.getVisibility(adapter, entry, "geometry"));
  }

  @Test
  public void testCatchAllRegexDefault() {
    entry.put("vis", "{\"pid\":\"TS\", \".*\":\"U\"}");
    assertEquals("U", visHandler.getVisibility(adapter, entry, "pop"));
  }
}
