/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.locationtech.geowave.core.index.Coordinate;
import org.locationtech.geowave.core.index.CoordinateRange;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRanges;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinates;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.numeric.NumericValue;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;

public class MockComponents {
  // Mock class instantiating abstract class so we can test logic
  // contained in abstract class.
  public static class MockAbstractDataAdapter implements
      DefaultStatisticsProvider,
      DataTypeAdapter<Integer> {
    private String id = ID;

    public MockAbstractDataAdapter() {
      this(ID);
    }

    public MockAbstractDataAdapter(final String id) {
      super();
      this.id = id;
      // final List<IndexFieldHandler<Integer, TestIndexFieldType, Object>> handlers =
      // new ArrayList<>();
      // handlers.add(new TestIndexFieldHandler());
      // super.init(handlers, null);
    }

    public static final String INTEGER = "TestInteger";
    public static final String ID = "TestIntegerAdapter";
    private static final FieldDescriptor<?>[] FIELDS =
        new FieldDescriptor[] {
            new FieldDescriptorBuilder<>(Integer.class).indexHint(
                TestDimensionField.TEST_DIMENSION_HINT).fieldName(INTEGER).build(),
            new FieldDescriptorBuilder<>(String.class).fieldName(ID).build()};

    @Override
    public String getTypeName() {
      return id;
    }

    @Override
    public byte[] getDataId(final Integer entry) {
      return StringUtils.stringToBinary("DataID" + entry.toString());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public FieldReader getReader(final String fieldId) {
      if (fieldId.equals(INTEGER)) {
        return FieldUtils.getDefaultReaderForClass(Integer.class);
      } else if (fieldId.equals(ID)) {
        return FieldUtils.getDefaultReaderForClass(String.class);
      }
      return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public FieldWriter getWriter(final String fieldId) {
      if (fieldId.equals(INTEGER)) {
        return FieldUtils.getDefaultWriterForClass(Integer.class);
      } else if (fieldId.equals(ID)) {
        return FieldUtils.getDefaultWriterForClass(String.class);
      }
      return null;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if ((o == null) || (getClass() != o.getClass())) {
        return false;
      }
      final MockAbstractDataAdapter that = (MockAbstractDataAdapter) o;
      return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }

    @Override
    public byte[] toBinary() {
      final byte[] idBinary = StringUtils.stringToBinary(id);
      return Bytes.concat(ByteBuffer.allocate(4).putInt(idBinary.length).array(), idBinary);
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final byte[] idBinary = new byte[buf.getInt()];
      buf.get(idBinary);
      id = StringUtils.stringFromBinary(idBinary);
    }

    @Override
    public RowBuilder<Integer> newRowBuilder(final FieldDescriptor<?>[] outputFieldDescriptors) {
      return new RowBuilder<Integer>() {
        @SuppressWarnings("unused")
        private String myid;

        private Integer intValue;

        @Override
        public void setField(final String id, final Object fieldValue) {
          if (id.equals(INTEGER)) {
            intValue = (Integer) fieldValue;
          } else if (id.equals(ID)) {
            myid = (String) fieldValue;
          }
        }

        @Override
        public void setFields(final Map<String, Object> values) {
          if (values.containsKey(INTEGER)) {
            intValue = (Integer) values.get(INTEGER);
          }
          if (values.containsKey(ID)) {
            myid = (String) values.get(ID);
          }
        }

        @Override
        public Integer buildRow(final byte[] dataId) {
          return new Integer(intValue);
        }
      };
    }

    @Override
    public Class<Integer> getDataClass() {
      return Integer.class;
    }

    @Override
    public List<Statistic<? extends StatisticValue<?>>> getDefaultStatistics() {
      final List<Statistic<? extends StatisticValue<?>>> statistics = Lists.newArrayList();
      final CountStatistic count = new CountStatistic(getTypeName());
      count.setInternal();
      statistics.add(count);
      return statistics;
    }

    @Override
    public Object getFieldValue(final Integer entry, final String fieldName) {
      switch (fieldName) {
        case INTEGER:
          return entry;
        case ID:
          return entry.toString();
        default:
          break;
      }
      return null;
    }

    @Override
    public FieldDescriptor<?>[] getFieldDescriptors() {
      return FIELDS;
    }

    @Override
    public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
      switch (fieldName) {
        case INTEGER:
          return FIELDS[0];
        case ID:
          return FIELDS[1];
        default:
          break;
      }
      return null;
    }
  } // class MockAbstractDataAdapter

  // *************************************************************************
  //
  // Test index field type for dimension.
  //
  // *************************************************************************
  public static class TestIndexFieldType {
    private final Integer indexValue;

    public TestIndexFieldType(final Integer _indexValue) {
      indexValue = _indexValue;
    }
  }

  public static class TestIndexFieldTypeMapper extends
      IndexFieldMapper<Integer, TestIndexFieldType> {

    @Override
    public TestIndexFieldType toIndex(List<Integer> nativeFieldValues) {
      return new TestIndexFieldType(nativeFieldValues.get(0));
    }

    @Override
    public void toAdapter(TestIndexFieldType indexFieldValue, RowBuilder<?> rowBuilder) {
      rowBuilder.setField(adapterFields[0], indexFieldValue.indexValue);
    }

    @Override
    public Class<TestIndexFieldType> indexFieldType() {
      return TestIndexFieldType.class;
    }

    @Override
    public Class<Integer> adapterFieldType() {
      return Integer.class;
    }

    @Override
    public short adapterFieldCount() {
      return 1;
    }

  }

  // *************************************************************************
  //
  // Test implementation on interface DimensionField for use by
  // TestIndexModel.
  //
  // *************************************************************************
  public static class TestDimensionField implements NumericDimensionField<TestIndexFieldType> {
    final String fieldName;
    public static String FIELD = "TestDimensionField1";

    public static IndexDimensionHint TEST_DIMENSION_HINT = new IndexDimensionHint("TEST_DIMENSION");

    public TestDimensionField() {
      fieldName = FIELD;
    }

    @Override
    public double normalize(final double value) {
      return 0;
    }

    @Override
    public BinRange[] getNormalizedRanges(final NumericData range) {
      return null;
    }

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public NumericData getNumericData(final TestIndexFieldType dataElement) {
      return new NumericValue(dataElement.indexValue);
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public FieldWriter<TestIndexFieldType> getWriter() {
      return new IntegerWriter();
    }

    @Override
    public FieldReader<TestIndexFieldType> getReader() {
      return new IntegerReader();
    }

    @Override
    public NumericDimensionDefinition getBaseDefinition() {
      return new TestDimensionField();
    }

    @Override
    public boolean isCompatibleWith(final Class<?> clazz) {
      return TestIndexFieldType.class.isAssignableFrom(clazz);
    }

    @Override
    public double getRange() {
      return 0;
    }

    @Override
    public double denormalize(final double value) {
      return 0;
    }

    @Override
    public NumericRange getDenormalizedRange(final BinRange range) {
      return null;
    }

    @Override
    public int getFixedBinIdSize() {
      return 0;
    }

    @Override
    public NumericRange getBounds() {
      return null;
    }

    @Override
    public NumericData getFullRange() {
      return null;
    }

    @Override
    public Class<TestIndexFieldType> getFieldClass() {
      return TestIndexFieldType.class;
    }

    @Override
    public Set<IndexDimensionHint> getDimensionHints() {
      return Sets.newHashSet(TEST_DIMENSION_HINT);
    }
  }

  public static class MockIndexStrategy implements NumericIndexStrategy {

    @Override
    public byte[] toBinary() {
      return new byte[] {};
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public QueryRanges getQueryRanges(
        final MultiDimensionalNumericData indexedRange,
        final IndexMetaData... hints) {
      return getQueryRanges(indexedRange, -1, hints);
    }

    @Override
    public QueryRanges getQueryRanges(
        final MultiDimensionalNumericData indexedRange,
        final int maxEstimatedRangeDecomposition,
        final IndexMetaData... hints) {
      return new QueryRanges();
    }

    @Override
    public InsertionIds getInsertionIds(final MultiDimensionalNumericData indexedData) {
      final List<byte[]> ids = new ArrayList<>();
      for (final NumericData data : indexedData.getDataPerDimension()) {
        ids.add(Double.toString(data.getCentroid()).getBytes());
      }
      return new InsertionIds(ids);
    }

    @Override
    public InsertionIds getInsertionIds(
        final MultiDimensionalNumericData indexedData,
        final int maxEstimatedDuplicateIds) {
      return this.getInsertionIds(indexedData);
    }

    @Override
    public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
      return null;
    }

    @Override
    public String getId() {
      return "Test";
    }

    @Override
    public double[] getHighestPrecisionIdRangePerDimension() {
      return new double[] {Integer.MAX_VALUE};
    }

    @Override
    public List<IndexMetaData> createMetaData() {
      return Collections.emptyList();
    }

    @Override
    public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
        final MultiDimensionalNumericData dataRange,
        final IndexMetaData... hints) {
      final CoordinateRange[][] coordinateRangesPerDimension =
          new CoordinateRange[dataRange.getDimensionCount()][];
      for (int d = 0; d < coordinateRangesPerDimension.length; d++) {
        coordinateRangesPerDimension[d] = new CoordinateRange[1];
        coordinateRangesPerDimension[d][0] =
            new CoordinateRange(
                dataRange.getMinValuesPerDimension()[0].longValue(),
                dataRange.getMaxValuesPerDimension()[0].longValue(),
                new byte[] {});
      }
      return new MultiDimensionalCoordinateRanges[] {
          new MultiDimensionalCoordinateRanges(new byte[] {}, coordinateRangesPerDimension)};
    }

    @Override
    public MultiDimensionalNumericData getRangeForId(
        final byte[] partitionKey,
        final byte[] sortKey) {
      return null;
    }

    @Override
    public byte[][] getInsertionPartitionKeys(final MultiDimensionalNumericData insertionData) {
      return null;
    }

    @Override
    public byte[][] getQueryPartitionKeys(
        final MultiDimensionalNumericData queryData,
        final IndexMetaData... hints) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public MultiDimensionalCoordinates getCoordinatesPerDimension(
        final byte[] partitionKey,
        final byte[] sortKey) {
      return new MultiDimensionalCoordinates(
          new byte[] {},
          new Coordinate[] {
              new Coordinate((long) Double.parseDouble(new String(sortKey)), new byte[] {})});
    }

    @Override
    public int getPartitionKeyLength() {
      return 0;
    }
  }

  // *************************************************************************
  //
  // Test index model class for use in testing encoding by
  // AbstractDataAdapter.
  //
  // *************************************************************************
  public static class TestIndexModel implements CommonIndexModel {

    private final TestDimensionField[] dimensionFields;
    private String id = "testmodel";

    public TestIndexModel() {
      dimensionFields = new TestDimensionField[1];
      dimensionFields[0] = new TestDimensionField();
    }

    public TestIndexModel(final String id) {
      dimensionFields = new TestDimensionField[1];
      dimensionFields[0] = new TestDimensionField();
      this.id = id;
    }

    @Override
    public FieldReader<Object> getReader(final String fieldName) {
      final FieldReader<?> reader = dimensionFields[0].getReader();
      return (FieldReader<Object>) reader;
    }

    @Override
    public FieldWriter<Object> getWriter(final String fieldName) {
      final FieldWriter<?> writer = dimensionFields[0].getWriter();
      return (FieldWriter<Object>) writer;
    }

    @Override
    public byte[] toBinary() {
      return new byte[] {};
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public TestDimensionField[] getDimensions() {
      return dimensionFields;
    }

    @Override
    public String getId() {
      return id;
    }
  }

  public static class IntegerReader implements FieldReader<TestIndexFieldType> {

    @Override
    public TestIndexFieldType readField(final byte[] fieldData) {
      return new TestIndexFieldType(Integer.parseInt(new String(fieldData)));
    }
  }

  public static class IntegerWriter implements FieldWriter<TestIndexFieldType> {

    @Override
    public byte[] writeField(final TestIndexFieldType fieldValue) {
      return Integer.toString(fieldValue.indexValue).getBytes();
    }
  }
}
