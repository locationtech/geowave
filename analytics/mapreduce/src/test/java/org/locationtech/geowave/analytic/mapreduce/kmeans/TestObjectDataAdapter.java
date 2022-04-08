/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kmeans;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptorBuilder;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.mapreduce.HadoopDataAdapter;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializer;
import org.locationtech.jts.geom.Geometry;
import com.google.common.base.Functions;

public class TestObjectDataAdapter implements HadoopDataAdapter<TestObject, TestObjectWritable> {
  private static final String GEOM = "myGeo";
  private static final String ID = "myId";
  private static final String GROUP_ID = "myGroupId";

  private static final FieldDescriptor<Geometry> GEO_FIELD =
      new SpatialFieldDescriptorBuilder<>(Geometry.class).fieldName(
          GEOM).spatialIndexHint().build();
  private static final FieldDescriptor<String> ID_FIELD =
      new FieldDescriptorBuilder<>(String.class).fieldName(ID).build();
  private static final FieldDescriptor<String> GROUP_ID_FIELD =
      new FieldDescriptorBuilder<>(String.class).fieldName(GROUP_ID).build();
  private static final FieldDescriptor<?>[] DESCRIPTORS =
      new FieldDescriptor[] {GEO_FIELD, ID_FIELD, GROUP_ID_FIELD};
  private static final Map<String, FieldDescriptor<?>> DESCRIPTOR_MAP =
      Arrays.stream(DESCRIPTORS).collect(
          Collectors.toMap(FieldDescriptor::fieldName, Functions.identity()));

  public TestObjectDataAdapter() {
    super();
  }

  @Override
  public String getTypeName() {
    return "test";
  }

  @Override
  public byte[] getDataId(final TestObject entry) {
    return StringUtils.stringToBinary(entry.id);
  }

  @Override
  public RowBuilder<TestObject> newRowBuilder(final FieldDescriptor<?>[] outputFieldDescriptors) {
    return new RowBuilder<TestObject>() {
      private String id;
      private String groupID;
      private Geometry geom;

      @Override
      public void setField(final String id, final Object fieldValue) {
        if (id.equals(GEOM)) {
          geom = (Geometry) fieldValue;
        } else if (id.equals(ID)) {
          this.id = (String) fieldValue;
        } else if (id.equals(GROUP_ID)) {
          groupID = (String) fieldValue;
        }
      }

      @Override
      public void setFields(final Map<String, Object> values) {
        if (values.containsKey(GEOM)) {
          geom = (Geometry) values.get(GEOM);
        }
        if (values.containsKey(ID)) {
          id = (String) values.get(ID);
        }
        if (values.containsKey(GROUP_ID)) {
          groupID = (String) values.get(GROUP_ID);
        }
      }

      @Override
      public TestObject buildRow(final byte[] dataId) {
        return new TestObject(geom, id, groupID);
      }
    };
  }

  @Override
  public HadoopWritableSerializer<TestObject, TestObjectWritable> createWritableSerializer() {
    return new TestObjectHadoopSerializer();
  }

  private class TestObjectHadoopSerializer implements
      HadoopWritableSerializer<TestObject, TestObjectWritable> {

    @Override
    public TestObjectWritable toWritable(final TestObject entry) {
      return new TestObjectWritable(entry);
    }

    @Override
    public TestObject fromWritable(final TestObjectWritable writable) {
      return writable.getObj();
    }
  }

  @Override
  public Object getFieldValue(final TestObject entry, final String fieldName) {
    switch (fieldName) {
      case GEOM:
        return entry.geo;
      case ID:
        return entry.id;
      case GROUP_ID:
        return entry.groupID;
    }
    return null;
  }

  @Override
  public Class<TestObject> getDataClass() {
    return TestObject.class;
  }

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return DESCRIPTORS;
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
    return DESCRIPTOR_MAP.get(fieldName);
  }

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {}
}
