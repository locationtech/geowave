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
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.index.NoOpIndexFieldMapper;
import com.google.common.collect.Lists;

public class IndexFieldMapperTest {

  @Test
  public void testNoOpIndexFieldMapper() {
    final NoOpIndexFieldMapper<Integer> mapper = new NoOpIndexFieldMapper<>(Integer.class);

    FieldDescriptor<Integer> testField =
        new FieldDescriptorBuilder<>(Integer.class).fieldName("testField").build();

    mapper.init("testIndexField", Lists.newArrayList(testField), null);

    assertEquals("testIndexField", mapper.indexFieldName());
    assertEquals(Integer.class, mapper.indexFieldType());
    assertEquals(Integer.class, mapper.adapterFieldType());
    assertEquals("testField", mapper.getAdapterFields()[0]);
    assertEquals(1, mapper.adapterFieldCount());
    List<Integer> adapterValues = mapper.toAdapter(42);
    assertEquals(1, adapterValues.size());
    assertEquals((int) 42, (int) adapterValues.get(0));
    adapterValues.clear();
    adapterValues.add(43);
    assertEquals((int) 43, (int) mapper.toIndex(adapterValues));

    final byte[] mapperBytes = PersistenceUtils.toBinary(mapper);

    final NoOpIndexFieldMapper<Integer> deserialized =
        (NoOpIndexFieldMapper) PersistenceUtils.fromBinary(mapperBytes);
    assertEquals("testIndexField", deserialized.indexFieldName());
    assertEquals(Integer.class, deserialized.indexFieldType());
    assertEquals(Integer.class, deserialized.adapterFieldType());
    assertEquals("testField", deserialized.getAdapterFields()[0]);
    assertEquals(1, deserialized.adapterFieldCount());
    adapterValues = deserialized.toAdapter(42);
    assertEquals(1, adapterValues.size());
    assertEquals((int) 42, (int) adapterValues.get(0));
    adapterValues.clear();
    adapterValues.add(43);
    assertEquals((int) 43, (int) deserialized.toIndex(adapterValues));
  }

}
