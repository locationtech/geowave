/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

public class BasicDataTypeAdapterTest {

  @Test
  public void testBasicDataTypeAdapter() {
    BasicDataTypeAdapter<TestType> adapter = new TestTypeBasicDataAdapter("myType");

    assertEquals("myType", adapter.getTypeName());
    assertEquals(TestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertEquals("name", adapter.getFieldDescriptors()[0].fieldName());
    assertEquals("doubleField", adapter.getFieldDescriptors()[1].fieldName());
    assertEquals("intField", adapter.getFieldDescriptors()[2].fieldName());
    assertEquals("boolField", adapter.getFieldDescriptors()[3].fieldName());

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(TestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertEquals("name", adapter.getFieldDescriptors()[0].fieldName());
    assertEquals("doubleField", adapter.getFieldDescriptors()[1].fieldName());
    assertEquals("intField", adapter.getFieldDescriptors()[2].fieldName());
    assertEquals("boolField", adapter.getFieldDescriptors()[3].fieldName());

    final TestType testEntry = new TestType("id1", 2.5, 8, true);
    assertEquals("id1", adapter.getFieldValue(testEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));
  }

  public static class TestType {
    public String name;
    public Double doubleField;
    public Integer intField;
    public Boolean boolField;

    public TestType(
        final String name,
        final Double doubleField,
        final Integer intField,
        final Boolean boolField) {
      this.name = name;
      this.doubleField = doubleField;
      this.intField = intField;
      this.boolField = boolField;
    }
  }

  public static class TestTypeBasicDataAdapter extends BasicDataTypeAdapter<TestType> {

    static final FieldDescriptor<?>[] fields =
        new FieldDescriptor<?>[] {
            new FieldDescriptorBuilder<>(String.class).fieldName("name").build(),
            new FieldDescriptorBuilder<>(Double.class).fieldName("doubleField").build(),
            new FieldDescriptorBuilder<>(Integer.class).fieldName("intField").indexHint(
                new IndexDimensionHint("test")).build(),
            new FieldDescriptorBuilder<>(Boolean.class).fieldName("boolField").build()};

    public TestTypeBasicDataAdapter() {}

    public TestTypeBasicDataAdapter(final String typeName) {
      super(typeName, fields, "name");
    }

    @Override
    public Object getFieldValue(TestType entry, String fieldName) {
      switch (fieldName) {
        case "name":
          return entry.name;
        case "doubleField":
          return entry.doubleField;
        case "intField":
          return entry.intField;
        case "boolField":
          return entry.boolField;
      }
      return null;
    }

    @Override
    public TestType buildObject(Object[] fieldValues) {
      return new TestType(
          (String) fieldValues[0],
          (Double) fieldValues[1],
          (Integer) fieldValues[2],
          (Boolean) fieldValues[3]);
    }

  }

}
