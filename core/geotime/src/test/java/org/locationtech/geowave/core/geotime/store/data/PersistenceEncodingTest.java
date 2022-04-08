/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptorBuilder;
import org.locationtech.geowave.core.geotime.adapter.TemporalFieldDescriptorBuilder;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

public class PersistenceEncodingTest {

  private final GeometryFactory factory =
      new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING));

  private static final NumericDimensionDefinition[] SPATIAL_TEMPORAL_DIMENSIONS =
      new NumericDimensionDefinition[] {
          new LongitudeDefinition(),
          new LatitudeDefinition(),
          new TimeDefinition(Unit.YEAR),};

  private static final CommonIndexModel model =
      SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
          new SpatialTemporalOptions()).getIndexModel();

  private static final NumericIndexStrategy strategy =
      TieredSFCIndexFactory.createSingleTierStrategy(
          SPATIAL_TEMPORAL_DIMENSIONS,
          new int[] {16, 16, 16},
          SFCType.HILBERT);

  private static final Index index = new IndexImpl(strategy, model);

  Date start = null, end = null;

  @Before
  public void setUp() throws ParseException {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    start = dateFormat.parse("2012-04-03 13:30:23.304");
    end = dateFormat.parse("2012-04-03 14:30:23.304");
  }

  @Test
  public void testPoint() {

    final GeoObjDataAdapter adapter = new GeoObjDataAdapter(false);
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) -1), index);

    final GeoObj entry =
        new GeoObj(factory.createPoint(new Coordinate(43.454, 28.232)), start, end, "g1");
    final List<byte[]> ids =
        adapter.asInternalAdapter((short) -1).encode(entry, indexMapping, index).getInsertionIds(
            index).getCompositeInsertionIds();

    assertEquals(1, ids.size());
  }

  @Test
  public void testLine() {

    final GeoObjDataAdapter adapter = new GeoObjDataAdapter(false);
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) -1), index);
    final GeoObj entry =
        new GeoObj(
            factory.createLineString(
                new Coordinate[] {new Coordinate(43.444, 28.232), new Coordinate(43.454, 28.242)}),
            start,
            end,
            "g1");
    final List<byte[]> ids =
        adapter.asInternalAdapter((short) -1).encode(entry, indexMapping, index).getInsertionIds(
            index).getCompositeInsertionIds();
    assertEquals(15, ids.size());
  }

  @Test
  public void testLineWithPrecisionOnTheTileEdge() {

    final NumericIndexStrategy strategy =
        TieredSFCIndexFactory.createSingleTierStrategy(
            SPATIAL_TEMPORAL_DIMENSIONS,
            new int[] {14, 14, 14},
            SFCType.HILBERT);

    final Index index = new IndexImpl(strategy, model);

    final GeoObjDataAdapter adapter = new GeoObjDataAdapter(false);

    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) -1), index);

    final GeoObj entry =
        new GeoObj(
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(-99.22, 33.75000000000001), // notice
                    // that
                    // this gets
                    // tiled as
                    // 33.75
                    new Coordinate(-99.15, 33.75000000000001)
                // notice that this gets tiled as 33.75
                }),
            new Date(352771200000l),
            new Date(352771200000l),
            "g1");
    final List<byte[]> ids =
        adapter.asInternalAdapter((short) -1).encode(entry, indexMapping, index).getInsertionIds(
            index).getCompositeInsertionIds();
    assertEquals(4, ids.size());
  }

  @Test
  public void testPoly() {
    final GeoObjDataAdapter adapter = new GeoObjDataAdapter(false);

    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) -1), index);
    final GeoObj entry =
        new GeoObj(
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(43.444, 28.232),
                    new Coordinate(43.454, 28.242),
                    new Coordinate(43.444, 28.252),
                    new Coordinate(43.444, 28.232),}),
            start,
            end,
            "g1");
    final List<byte[]> ids =
        adapter.asInternalAdapter((short) -1).encode(entry, indexMapping, index).getInsertionIds(
            index).getCompositeInsertionIds();
    assertEquals(27, ids.size());
  }

  @Test
  public void testPointRange() {

    final GeoObjDataAdapter adapter = new GeoObjDataAdapter(true);
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) -1), index);

    final GeoObj entry =
        new GeoObj(factory.createPoint(new Coordinate(43.454, 28.232)), start, end, "g1");
    final List<byte[]> ids =
        adapter.asInternalAdapter((short) -1).encode(entry, indexMapping, index).getInsertionIds(
            index).getCompositeInsertionIds();

    assertEquals(8, ids.size());
  }

  @Test
  public void testLineRnge() {

    final GeoObjDataAdapter adapter = new GeoObjDataAdapter(true);
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) -1), index);
    final GeoObj entry =
        new GeoObj(
            factory.createLineString(
                new Coordinate[] {new Coordinate(43.444, 28.232), new Coordinate(43.454, 28.242)}),
            start,
            end,
            "g1");
    final List<byte[]> ids =
        adapter.asInternalAdapter((short) -1).encode(entry, indexMapping, index).getInsertionIds(
            index).getCompositeInsertionIds();
    assertTrue(ids.size() < 200);
  }

  private static final String GEOM = "myGeo";
  private static final String ID = "myId";
  private static final String START_TIME = "startTime";
  private static final String END_TIME = "endTime";
  private static final FieldDescriptor<Geometry> GEO_FIELD =
      new SpatialFieldDescriptorBuilder<>(Geometry.class).spatialIndexHint().fieldName(
          GEOM).build();
  private static final FieldDescriptor<String> ID_FIELD =
      new FieldDescriptorBuilder<>(String.class).fieldName(ID).build();

  // Time fields for time instant tests
  private static final FieldDescriptor<Date> START_TIME_FIELD =
      new TemporalFieldDescriptorBuilder<>(Date.class).timeIndexHint().fieldName(
          START_TIME).build();
  private static final FieldDescriptor<Date> END_TIME_FIELD =
      new TemporalFieldDescriptorBuilder<>(Date.class).fieldName(END_TIME).build();

  // Time fields for time range tests
  private static final FieldDescriptor<Date> START_TIME_RANGE_FIELD =
      new TemporalFieldDescriptorBuilder<>(Date.class).startTimeIndexHint().fieldName(
          START_TIME).build();
  private static final FieldDescriptor<Date> END_TIME_RANGE_FIELD =
      new TemporalFieldDescriptorBuilder<>(Date.class).endTimeIndexHint().fieldName(
          END_TIME).build();

  private static final FieldDescriptor<?>[] TIME_DESCRIPTORS =
      new FieldDescriptor[] {GEO_FIELD, ID_FIELD, START_TIME_FIELD, END_TIME_FIELD};

  private static final FieldDescriptor<?>[] TIME_RANGE_DESCRIPTORS =
      new FieldDescriptor[] {GEO_FIELD, ID_FIELD, START_TIME_RANGE_FIELD, END_TIME_RANGE_FIELD};

  public static class GeoObjDataAdapter implements DataTypeAdapter<GeoObj> {
    private boolean isTimeRange;

    public GeoObjDataAdapter() {
      this(false);
    }

    public GeoObjDataAdapter(final boolean isTimeRange) {
      super();
      this.isTimeRange = isTimeRange;
    }

    @Override
    public String getTypeName() {
      return "geoobj";
    }

    @Override
    public byte[] getDataId(final GeoObj entry) {
      return entry.id.getBytes();
    }

    @Override
    public RowBuilder<GeoObj> newRowBuilder(final FieldDescriptor<?>[] outputFieldDescriptors) {
      return new RowBuilder<GeoObj>() {
        private String id;
        private Geometry geom;
        private Date stime;
        private Date etime;

        @Override
        public void setField(final String id, final Object fieldValue) {
          if (id.equals(GEOM)) {
            geom = (Geometry) fieldValue;
          } else if (id.equals(ID)) {
            this.id = (String) fieldValue;
          } else if (id.equals(START_TIME)) {
            stime = (Date) fieldValue;
          } else {
            etime = (Date) fieldValue;
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
          if (values.containsKey(START_TIME)) {
            stime = (Date) values.get(START_TIME);
          }
          if (values.containsKey(END_TIME)) {
            etime = (Date) values.get(END_TIME);
          }
        }

        @Override
        public GeoObj buildRow(final byte[] dataId) {
          return new GeoObj(geom, stime, etime, id);
        }
      };
    }

    @Override
    public Object getFieldValue(final GeoObj entry, final String fieldName) {
      switch (fieldName) {
        case GEOM:
          return entry.geometry;
        case ID:
          return entry.id;
        case START_TIME:
          return entry.startTime;
        case END_TIME:
          return entry.endTime;
      }
      return null;
    }

    @Override
    public Class<GeoObj> getDataClass() {
      return GeoObj.class;
    }

    @Override
    public byte[] toBinary() {
      return new byte[] {isTimeRange ? (byte) 1 : 0};
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      isTimeRange = bytes[0] == 1;
    }

    @Override
    public FieldDescriptor<?>[] getFieldDescriptors() {
      return isTimeRange ? TIME_RANGE_DESCRIPTORS : TIME_DESCRIPTORS;
    }

    @Override
    public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
      return Arrays.stream(isTimeRange ? TIME_RANGE_DESCRIPTORS : TIME_DESCRIPTORS).filter(
          field -> field.fieldName().equals(fieldName)).findFirst().orElse(null);
    }
  }

  private static class GeoObj {
    private final Geometry geometry;
    private final String id;
    private final Date startTime;
    private final Date endTime;

    public GeoObj(
        final Geometry geometry,
        final Date startTime,
        final Date endTime,
        final String id) {
      super();
      this.geometry = geometry;
      this.startTime = startTime;
      this.endTime = endTime;
      this.id = id;
    }
  }
}
