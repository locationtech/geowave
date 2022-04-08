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
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

public class FieldDescriptorTest {

  @Test
  public void testFieldDescriptor() {
    final FieldDescriptor<String> testDescriptor =
        new FieldDescriptorBuilder<>(String.class).fieldName("testFieldName").indexHint(
            new IndexDimensionHint("testDimensionHint")).build();

    assertEquals("testFieldName", testDescriptor.fieldName());
    assertEquals(String.class, testDescriptor.bindingClass());
    assertEquals(1, testDescriptor.indexHints().size());
    assertTrue(testDescriptor.indexHints().contains(new IndexDimensionHint("testDimensionHint")));

    final byte[] fieldDescriptorBytes = PersistenceUtils.toBinary(testDescriptor);
    final FieldDescriptor<?> deserialized =
        (FieldDescriptor<?>) PersistenceUtils.fromBinary(fieldDescriptorBytes);

    assertEquals("testFieldName", deserialized.fieldName());
    assertEquals(String.class, deserialized.bindingClass());
    assertEquals(1, deserialized.indexHints().size());
    assertTrue(deserialized.indexHints().contains(new IndexDimensionHint("testDimensionHint")));
  }

}
