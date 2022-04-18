/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;

public class BasicDataTypeAdapterTest {
  @Test
  public void testObjectBasedDataAdapter() {
    BasicDataTypeAdapter<TestType> adapter =
        BasicDataTypeAdapter.newAdapter("myType", TestType.class, "name");

    assertEquals("myType", adapter.getTypeName());
    assertEquals(TestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("name"));
    assertTrue(String.class.isAssignableFrom(adapter.getFieldDescriptor("name").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(TestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("name"));
    assertTrue(String.class.isAssignableFrom(adapter.getFieldDescriptor("name").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));

    final TestType testEntry = new TestType("id1", 2.5, 8, true);
    assertEquals("id1", adapter.getFieldValue(testEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));

    final Object[] fields = new Object[4];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "name":
          fields[i] = "id1";
          break;
        case "doubleField":
          fields[i] = 2.5;
          break;
        case "intField":
          fields[i] = 8;
          break;
        case "boolField":
          fields[i] = true;
          break;
      }
    }

    final TestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(builtEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(builtEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(builtEntry, "boolField"));
  }

  @Test
  public void testInheritedObjectBasedDataAdapter() {
    BasicDataTypeAdapter<InheritedTestType> adapter =
        BasicDataTypeAdapter.newAdapter("myType", InheritedTestType.class, "name");

    assertEquals("myType", adapter.getTypeName());
    assertEquals(InheritedTestType.class, adapter.getDataClass());
    assertEquals(5, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("name"));
    assertTrue(String.class.isAssignableFrom(adapter.getFieldDescriptor("name").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("extraField"));
    assertTrue(
        String.class.isAssignableFrom(adapter.getFieldDescriptor("extraField").bindingClass()));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(InheritedTestType.class, adapter.getDataClass());
    assertEquals(5, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("name"));
    assertTrue(String.class.isAssignableFrom(adapter.getFieldDescriptor("name").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("extraField"));
    assertTrue(
        String.class.isAssignableFrom(adapter.getFieldDescriptor("extraField").bindingClass()));

    final InheritedTestType testEntry = new InheritedTestType("id1", 2.5, 8, true, "extra");
    assertEquals("id1", adapter.getFieldValue(testEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));
    assertEquals("extra", adapter.getFieldValue(testEntry, "extraField"));

    final Object[] fields = new Object[5];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "name":
          fields[i] = "id1";
          break;
        case "doubleField":
          fields[i] = 2.5;
          break;
        case "intField":
          fields[i] = 8;
          break;
        case "boolField":
          fields[i] = true;
          break;
        case "extraField":
          fields[i] = "extra";
          break;
      }
    }

    final InheritedTestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(builtEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(builtEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(builtEntry, "boolField"));
    assertEquals("extra", adapter.getFieldValue(builtEntry, "extraField"));
  }

  @Test
  public void testAnnotatedObjectBasedDataAdapter() {
    BasicDataTypeAdapter<AnnotatedTestType> adapter =
        BasicDataTypeAdapter.newAdapter("myType", AnnotatedTestType.class, "alternateName");

    assertEquals("myType", adapter.getTypeName());
    assertEquals(AnnotatedTestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("alternateName"));
    assertTrue(
        String.class.isAssignableFrom(adapter.getFieldDescriptor("alternateName").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("alternateName").indexHints().contains(
            new IndexDimensionHint("a")));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("a")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("b")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("c")));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(AnnotatedTestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("alternateName"));
    assertTrue(
        String.class.isAssignableFrom(adapter.getFieldDescriptor("alternateName").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("alternateName").indexHints().contains(
            new IndexDimensionHint("a")));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("a")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("b")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("c")));

    final AnnotatedTestType testEntry = new AnnotatedTestType("id1", 2.5, 8, true, "ignored");
    assertEquals("id1", adapter.getFieldValue(testEntry, "alternateName"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));

    final Object[] fields = new Object[4];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "alternateName":
          fields[i] = "id1";
          break;
        case "doubleField":
          fields[i] = 2.5;
          break;
        case "intField":
          fields[i] = 8;
          break;
        case "boolField":
          fields[i] = true;
          break;
      }
    }

    final AnnotatedTestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "alternateName"));
    assertEquals(2.5, (double) adapter.getFieldValue(builtEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(builtEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(builtEntry, "boolField"));
    assertNull(builtEntry.ignoredField);
  }

  @Test
  public void testInheritedAnnotatedObjectBasedDataAdapter() {
    BasicDataTypeAdapter<InheritedAnnotatedTestType> adapter =
        BasicDataTypeAdapter.newAdapter(
            "myType",
            InheritedAnnotatedTestType.class,
            "alternateName");

    assertEquals("myType", adapter.getTypeName());
    assertEquals(InheritedAnnotatedTestType.class, adapter.getDataClass());
    assertEquals(5, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("alternateName"));
    assertTrue(
        String.class.isAssignableFrom(adapter.getFieldDescriptor("alternateName").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("alternateName").indexHints().contains(
            new IndexDimensionHint("a")));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("a")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("b")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("c")));
    assertNotNull(adapter.getFieldDescriptor("extraField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("extraField").bindingClass()));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(InheritedAnnotatedTestType.class, adapter.getDataClass());
    assertEquals(5, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("alternateName"));
    assertTrue(
        String.class.isAssignableFrom(adapter.getFieldDescriptor("alternateName").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("alternateName").indexHints().contains(
            new IndexDimensionHint("a")));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("a")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("b")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("c")));
    assertNotNull(adapter.getFieldDescriptor("extraField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("extraField").bindingClass()));

    final InheritedAnnotatedTestType testEntry =
        new InheritedAnnotatedTestType("id1", 2.5, 8, true, "ignored", 5.3);
    assertEquals("id1", adapter.getFieldValue(testEntry, "alternateName"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));
    assertEquals(5.3, (double) adapter.getFieldValue(testEntry, "extraField"), 0.001);

    final Object[] fields = new Object[5];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "alternateName":
          fields[i] = "id1";
          break;
        case "doubleField":
          fields[i] = 2.5;
          break;
        case "intField":
          fields[i] = 8;
          break;
        case "boolField":
          fields[i] = true;
          break;
        case "extraField":
          fields[i] = 5.3;
          break;
      }
    }

    final InheritedAnnotatedTestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "alternateName"));
    assertEquals(2.5, (double) adapter.getFieldValue(builtEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(builtEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(builtEntry, "boolField"));
    assertEquals(5.3, (double) adapter.getFieldValue(builtEntry, "extraField"), 0.001);
    assertNull(builtEntry.ignoredField);
  }

  @Test
  public void testObjectBasedDataAdapterSeparateDataID() {
    BasicDataTypeAdapter<TestType> adapter =
        BasicDataTypeAdapter.newAdapter("myType", TestType.class, "name", true);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(TestType.class, adapter.getDataClass());
    assertEquals(3, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("name"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(TestType.class, adapter.getDataClass());
    assertEquals(3, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("name"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));

    final TestType testEntry = new TestType("id1", 2.5, 8, true);
    assertEquals("id1", adapter.getFieldValue(testEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));

    final Object[] fields = new Object[3];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "doubleField":
          fields[i] = 2.5;
          break;
        case "intField":
          fields[i] = 8;
          break;
        case "boolField":
          fields[i] = true;
          break;
      }
    }

    final TestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(builtEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(builtEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(builtEntry, "boolField"));
  }

  @Test
  public void testInheritedObjectBasedDataAdapterSeparateDataID() {
    BasicDataTypeAdapter<InheritedTestType> adapter =
        BasicDataTypeAdapter.newAdapter("myType", InheritedTestType.class, "name", true);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(InheritedTestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("name"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("extraField"));
    assertTrue(
        String.class.isAssignableFrom(adapter.getFieldDescriptor("extraField").bindingClass()));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(InheritedTestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("name"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("extraField"));
    assertTrue(
        String.class.isAssignableFrom(adapter.getFieldDescriptor("extraField").bindingClass()));

    final InheritedTestType testEntry = new InheritedTestType("id1", 2.5, 8, true, "extra");
    assertEquals("id1", adapter.getFieldValue(testEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));
    assertEquals("extra", adapter.getFieldValue(testEntry, "extraField"));

    final Object[] fields = new Object[4];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "doubleField":
          fields[i] = 2.5;
          break;
        case "intField":
          fields[i] = 8;
          break;
        case "boolField":
          fields[i] = true;
          break;
        case "extraField":
          fields[i] = "extra";
          break;
      }
    }

    final InheritedTestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "name"));
    assertEquals(2.5, (double) adapter.getFieldValue(builtEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(builtEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(builtEntry, "boolField"));
    assertEquals("extra", adapter.getFieldValue(builtEntry, "extraField"));
  }

  @Test
  public void testAnnotatedObjectBasedDataAdapterSeparateDataID() {
    BasicDataTypeAdapter<AnnotatedTestType> adapter =
        BasicDataTypeAdapter.newAdapter("myType", AnnotatedTestType.class, "alternateName", true);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(AnnotatedTestType.class, adapter.getDataClass());
    assertEquals(3, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("alternateName"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));
    assertTrue(
        adapter.getDataIDFieldDescriptor().indexHints().contains(new IndexDimensionHint("a")));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("a")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("b")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("c")));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(AnnotatedTestType.class, adapter.getDataClass());
    assertEquals(3, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("alternateName"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));
    assertTrue(
        adapter.getDataIDFieldDescriptor().indexHints().contains(new IndexDimensionHint("a")));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("a")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("b")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("c")));

    final AnnotatedTestType testEntry = new AnnotatedTestType("id1", 2.5, 8, true, "ignored");
    assertEquals("id1", adapter.getFieldValue(testEntry, "alternateName"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));

    final Object[] fields = new Object[3];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "doubleField":
          fields[i] = 2.5;
          break;
        case "intField":
          fields[i] = 8;
          break;
        case "boolField":
          fields[i] = true;
          break;
      }
    }

    final AnnotatedTestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "alternateName"));
    assertEquals(2.5, (double) adapter.getFieldValue(builtEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(builtEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(builtEntry, "boolField"));
    assertNull(builtEntry.ignoredField);
  }

  @Test
  public void testInheritedAnnotatedObjectBasedDataAdapterSeparateDataID() {
    BasicDataTypeAdapter<InheritedAnnotatedTestType> adapter =
        BasicDataTypeAdapter.newAdapter(
            "myType",
            InheritedAnnotatedTestType.class,
            "alternateName",
            true);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(InheritedAnnotatedTestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("alternateName"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));
    assertTrue(
        adapter.getDataIDFieldDescriptor().indexHints().contains(new IndexDimensionHint("a")));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("a")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("b")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("c")));
    assertNotNull(adapter.getFieldDescriptor("extraField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("extraField").bindingClass()));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(InheritedAnnotatedTestType.class, adapter.getDataClass());
    assertEquals(4, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("alternateName"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));
    assertTrue(
        adapter.getDataIDFieldDescriptor().indexHints().contains(new IndexDimensionHint("a")));
    assertNotNull(adapter.getFieldDescriptor("doubleField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("doubleField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("intField"));
    assertTrue(
        Integer.class.isAssignableFrom(adapter.getFieldDescriptor("intField").bindingClass()));
    assertNotNull(adapter.getFieldDescriptor("boolField"));
    assertTrue(
        Boolean.class.isAssignableFrom(adapter.getFieldDescriptor("boolField").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("a")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("b")));
    assertTrue(
        adapter.getFieldDescriptor("boolField").indexHints().contains(new IndexDimensionHint("c")));
    assertNotNull(adapter.getFieldDescriptor("extraField"));
    assertTrue(
        Double.class.isAssignableFrom(adapter.getFieldDescriptor("extraField").bindingClass()));

    final InheritedAnnotatedTestType testEntry =
        new InheritedAnnotatedTestType("id1", 2.5, 8, true, "ignored", 5.3);
    assertEquals("id1", adapter.getFieldValue(testEntry, "alternateName"));
    assertEquals(2.5, (double) adapter.getFieldValue(testEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(testEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(testEntry, "boolField"));
    assertEquals(5.3, (double) adapter.getFieldValue(testEntry, "extraField"), 0.001);

    final Object[] fields = new Object[4];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "doubleField":
          fields[i] = 2.5;
          break;
        case "intField":
          fields[i] = 8;
          break;
        case "boolField":
          fields[i] = true;
          break;
        case "extraField":
          fields[i] = 5.3;
          break;
      }
    }

    final InheritedAnnotatedTestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "alternateName"));
    assertEquals(2.5, (double) adapter.getFieldValue(builtEntry, "doubleField"), 0.001);
    assertEquals(8, adapter.getFieldValue(builtEntry, "intField"));
    assertTrue((boolean) adapter.getFieldValue(builtEntry, "boolField"));
    assertEquals(5.3, (double) adapter.getFieldValue(builtEntry, "extraField"), 0.001);
    assertNull(builtEntry.ignoredField);
  }

  @Test
  public void testSingleFieldDataAdapterSeparateDataID() {
    BasicDataTypeAdapter<SingleFieldTestType> adapter =
        BasicDataTypeAdapter.newAdapter("myType", SingleFieldTestType.class, "name", true);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(SingleFieldTestType.class, adapter.getDataClass());
    assertEquals(0, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("name"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(SingleFieldTestType.class, adapter.getDataClass());
    assertEquals(0, adapter.getFieldDescriptors().length);
    assertNull(adapter.getFieldDescriptor("name"));
    assertNotNull(adapter.getDataIDFieldDescriptor());
    assertTrue(String.class.isAssignableFrom(adapter.getDataIDFieldDescriptor().bindingClass()));

    final SingleFieldTestType testEntry = new SingleFieldTestType("id1");
    assertEquals("id1", adapter.getFieldValue(testEntry, "name"));

    final SingleFieldTestType builtEntry = adapter.buildObject("id1", new Object[0]);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "name"));
  }

  public static class TestType {
    private String name;
    private double doubleField;
    public int intField;
    public boolean boolField;

    protected TestType() {}

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

    public void setName(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setDoubleField(final double doubleField) {
      this.doubleField = doubleField;
    }

    public double getDoubleField() {
      return doubleField;
    }
  }

  public static class InheritedTestType extends TestType {
    public String extraField;

    public InheritedTestType() {
      super();
    }

    public InheritedTestType(
        final String name,
        final Double doubleField,
        final Integer intField,
        final Boolean boolField,
        final String extraField) {
      super(name, doubleField, intField, boolField);
      this.extraField = extraField;
    }
  }

  @GeoWaveDataType
  public static class AnnotatedTestType {
    @GeoWaveField(name = "alternateName", indexHints = "a")
    private String name;

    @GeoWaveField()
    private double doubleField;

    @GeoWaveField()
    private int intField;

    @GeoWaveField(indexHints = {"a", "b", "c"})
    private boolean boolField;

    protected String ignoredField;

    protected AnnotatedTestType() {}

    public AnnotatedTestType(
        final String name,
        final double doubleField,
        final int intField,
        final boolean boolField,
        final String ignoredField) {
      this.name = name;
      this.doubleField = doubleField;
      this.intField = intField;
      this.boolField = boolField;
      this.ignoredField = ignoredField;
    }
  }

  @GeoWaveDataType
  public static class InheritedAnnotatedTestType extends AnnotatedTestType {

    @GeoWaveField()
    private Double extraField;

    protected InheritedAnnotatedTestType() {
      super();
    }

    public InheritedAnnotatedTestType(
        final String name,
        final Double doubleField,
        final Integer intField,
        final Boolean boolField,
        final String ignoredField,
        final Double extraField) {
      super(name, doubleField, intField, boolField, ignoredField);
      this.extraField = extraField;
    }
  }

  public static class SingleFieldTestType {
    private String name;

    protected SingleFieldTestType() {}

    public SingleFieldTestType(final String name) {
      this.name = name;
    }

    public void setName(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
