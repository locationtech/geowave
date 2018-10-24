/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.adapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Coordinate;
import org.locationtech.geowave.core.index.CoordinateRange;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRanges;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinates;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.FieldNameStatisticVisibility;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.NumericRangeDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

import com.google.common.collect.Lists;

public class MockComponents
{
	// Mock class instantiating abstract class so we can test logic
	// contained in abstract class.
	public static class MockAbstractDataAdapter extends
			AbstractDataAdapter<Integer> implements
			StatisticsProvider<Integer>
	{
		private String id = ID;

		public MockAbstractDataAdapter() {
			this(
					ID);
		}

		public MockAbstractDataAdapter(
				final String id ) {
			super(
					Lists.newArrayList(),
					Lists.newArrayList());
			this.id = id;
			final List<IndexFieldHandler<Integer, TestIndexFieldType, Object>> handlers = new ArrayList<>();
			handlers.add(new TestIndexFieldHandler());
			super.init(
					handlers,
					null);
		}

		public static class TestIndexFieldHandler implements
				IndexFieldHandler<Integer, TestIndexFieldType, Object>,
				Persistable
		{
			@Override
			public String[] getNativeFieldNames() {
				return new String[] {
					INTEGER
				};
			}

			@Override
			public TestIndexFieldType toIndexValue(
					final Integer row ) {
				return new TestIndexFieldType(
						row);
			}

			@Override
			public PersistentValue<Object>[] toNativeValues(
					final TestIndexFieldType indexValue ) {
				return new PersistentValue[] {
					new PersistentValue<>(
							INTEGER,
							indexValue.indexValue)
				};
			}

			@Override
			public byte[] toBinary() {
				return new byte[0];
			}

			@Override
			public void fromBinary(
					byte[] bytes ) {}

		}

		protected static final String INTEGER = "TestInteger";
		protected static final String ID = "TestIntegerAdapter";

		public MockAbstractDataAdapter(
				final List<PersistentIndexFieldHandler<Integer, // RowType
				? extends CommonIndexValue, // IndexFieldType
				Object // NativeFieldType
				>> _indexFieldHandlers,
				final List<NativeFieldHandler<Integer, // RowType
				Object // FieldType
				>> _nativeFieldHandlers ) {
			super(
					_indexFieldHandlers,
					_nativeFieldHandlers);
		}

		@Override
		public String getTypeName() {
			return id;
		}

		@Override
		public ByteArray getDataId(
				final Integer entry ) {
			return new ByteArray(
					"DataID" + entry.toString());
		}

		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public FieldReader getReader(
				final String fieldId ) {
			if (fieldId.equals(INTEGER)) {
				return FieldUtils.getDefaultReaderForClass(Integer.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultReaderForClass(String.class);
			}
			return null;
		}

		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public FieldWriter getWriter(
				final String fieldId ) {
			if (fieldId.equals(INTEGER)) {
				return FieldUtils.getDefaultWriterForClass(Integer.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultWriterForClass(String.class);
			}
			return null;
		}

		@Override
		protected RowBuilder<Integer, Object> newBuilder() {
			return new RowBuilder<Integer, Object>() {
				@SuppressWarnings("unused")
				private String myid;
				private Integer intValue;

				@Override
				public void setField(
						final String id,
						final Object fieldValue ) {
					if (id.equals(INTEGER)) {
						intValue = (Integer) fieldValue;
					}
					else if (id.equals(ID)) {
						myid = (String) fieldValue;
					}
				}

				@Override
				public void setFields(
						final Map<String, Object> values ) {
					if (values.containsKey(INTEGER)) {
						intValue = (Integer) values.get(INTEGER);
					}
					if (values.containsKey(ID)) {
						myid = (String) values.get(ID);
					}
				}

				@Override
				public Integer buildRow(
						final ByteArray dataId ) {
					return new Integer(
							intValue);
				}
			};
		}

		@Override
		public StatisticsId[] getSupportedStatistics() {
			return new StatisticsId[] {
				CountDataStatistics.STATS_TYPE.newBuilder().build().getId(),

			};
		}

		@Override
		public <R, B extends StatisticsQueryBuilder<R, B>> InternalDataStatistics<Integer, R, B> createDataStatistics(
				final StatisticsId statisticsId ) {
			if (statisticsId.getType().equals(
					CountDataStatistics.STATS_TYPE)) {
				return (InternalDataStatistics<Integer, R, B>) new CountDataStatistics<Integer>();
			}
			return (InternalDataStatistics<Integer, R, B>) new IntegerRangeDataStatistics(
					getTypeName());
		}

		@Override
		public int getPositionOfOrderedField(
				final CommonIndexModel model,
				final String fieldName ) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (fieldName.equals(dimensionField.getFieldName())) {
					return i;
				}
				i++;
			}
			if (fieldName.equals(INTEGER)) {
				return i;
			}
			else if (fieldName.equals(ID)) {
				return i + 1;
			}
			return -1;
		}

		@Override
		public String getFieldNameForPosition(
				final CommonIndexModel model,
				final int position ) {
			if (position < model.getDimensions().length) {
				int i = 0;
				for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
					if (i == position) {
						return dimensionField.getFieldName();
					}
					i++;
				}
			}
			else {
				final int numDimensions = model.getDimensions().length;
				if (position == numDimensions) {
					return INTEGER;
				}
				else if (position == (numDimensions + 1)) {
					return ID;
				}
			}
			return null;
		}

		@Override
		public EntryVisibilityHandler<Integer> getVisibilityHandler(
				final CommonIndexModel indexModel,
				final DataTypeAdapter<Integer> adapter,
				final StatisticsId statisticsId ) {
			return new FieldNameStatisticVisibility<>(
					new TestDimensionField().fieldName,
					indexModel,
					adapter);
		}

	} // class MockAbstractDataAdapter

	public static class IntegerRangeDataStatistics extends
			NumericRangeDataStatistics<Integer>
	{
		protected static final FieldStatisticsType<Range<Double>> TYPE = new FieldStatisticsType<>(
				"Integer_Range");

		public IntegerRangeDataStatistics() {
			super();
		}

		public IntegerRangeDataStatistics(
				final String fieldName ) {
			super(
					null,
					TYPE,
					fieldName);
		}

		@Override
		protected NumericRange getRange(
				final Integer entry ) {
			return new NumericRange(
					entry.doubleValue(),
					entry.doubleValue());
		}
	}

	// *************************************************************************
	//
	// Test index field type for dimension.
	//
	// *************************************************************************
	public static class TestIndexFieldType implements
			CommonIndexValue
	{
		private final Integer indexValue;

		public TestIndexFieldType(
				final Integer _indexValue ) {
			indexValue = _indexValue;
		}

		@Override
		public byte[] getVisibility() {
			return null;
		}

		@Override
		public void setVisibility(
				final byte[] visibility ) {}

		@Override
		public boolean overlaps(
				final NumericDimensionField[] dimensions,
				final NumericData[] rangeData ) {
			return (rangeData[0].getMin() <= indexValue) && (rangeData[0].getMax() >= indexValue);
		}

	}

	// *************************************************************************
	//
	// Test class that implements PersistentIndexFieldHandler<T> for
	// instantiation of MockAbstractDataAdapter object.
	//
	// *************************************************************************
	public static class TestPersistentIndexFieldHandler implements
			PersistentIndexFieldHandler<Integer, TestIndexFieldType, Object>
	{

		public TestPersistentIndexFieldHandler() {}

		@Override
		public String[] getNativeFieldNames() {
			return new String[] {
				MockAbstractDataAdapter.INTEGER
			};
		}

		// toIndexValue simply increments each digit in number.
		@Override
		public TestIndexFieldType toIndexValue(
				final Integer row ) {

			final String sRow = row.toString();
			final int numDigits = sRow.length();
			String sNewRow = new String();
			final char[] newDigit = new char[1];
			for (int i = 0; i < numDigits; i++) {
				final char digit = sRow.charAt(i);
				switch (digit) {
					case '0':
						newDigit[0] = '1';
						break;
					case '1':
						newDigit[0] = '2';
						break;
					case '2':
						newDigit[0] = '3';
						break;
					case '3':
						newDigit[0] = '4';
						break;
					case '4':
						newDigit[0] = '5';
						break;
					case '5':
						newDigit[0] = '6';
						break;
					case '6':
						newDigit[0] = '7';
						break;
					case '7':
						newDigit[0] = '8';
						break;
					case '8':
						newDigit[0] = '9';
						break;
					case '9':
						newDigit[0] = '0';
						break;
				}
				sNewRow = sNewRow.concat(new String(
						newDigit));
			}
			return new TestIndexFieldType(
					Integer.decode(sNewRow));
		}

		// toNativeValues decrements each digit in the value.
		@SuppressWarnings("unchecked")
		@Override
		public PersistentValue<Object>[] toNativeValues(
				final TestIndexFieldType _indexValue ) {

			final String sRow = _indexValue.indexValue.toString();
			final int numDigits = sRow.length();
			String sNewRow = new String();
			final char[] newDigit = new char[1];
			for (int i = 0; i < numDigits; i++) {
				final char digit = sRow.charAt(i);
				switch (digit) {
					case '0':
						newDigit[0] = '9';
						break;
					case '1':
						newDigit[0] = '0';
						break;
					case '2':
						newDigit[0] = '1';
						break;
					case '3':
						newDigit[0] = '2';
						break;
					case '4':
						newDigit[0] = '3';
						break;
					case '5':
						newDigit[0] = '4';
						break;
					case '6':
						newDigit[0] = '5';
						break;
					case '7':
						newDigit[0] = '6';
						break;
					case '8':
						newDigit[0] = '7';
						break;
					case '9':
						newDigit[0] = '8';
						break;
				}
				sNewRow = sNewRow.concat(new String(
						newDigit));
			}
			final Integer newValue = Integer.decode(sNewRow);

			return new PersistentValue[] {
				new PersistentValue<Object>(
						MockAbstractDataAdapter.INTEGER,
						newValue)
			};
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}
	}

	// *************************************************************************
	//
	// Test class that implements NativeFieldHandler<RowType,FieldType>
	// for instantiation of MockAbstractDataAdapter object.
	//
	// *************************************************************************
	public static class TestNativeFieldHandler implements
			NativeFieldHandler<Integer, Object>
	{

		@Override
		public String getFieldName() {
			return MockAbstractDataAdapter.INTEGER;
		}

		@Override
		public Object getFieldValue(
				final Integer row ) {
			return row;
		}

	}

	// *************************************************************************
	//
	// Test implementation on interface DimensionField for use by
	// TestIndexModel.
	//
	// *************************************************************************
	public static class TestDimensionField implements
			NumericDimensionField<TestIndexFieldType>
	{
		final String fieldName;

		public TestDimensionField() {
			fieldName = "TestDimensionField1";
		}

		@Override
		public double normalize(
				final double value ) {
			return 0;
		}

		@Override
		public BinRange[] getNormalizedRanges(
				final NumericData range ) {
			return null;
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

		@Override
		public NumericData getNumericData(
				final TestIndexFieldType dataElement ) {
			return new NumericValue(
					dataElement.indexValue);
		}

		@Override
		public String getFieldName() {
			return fieldName;
		}

		@Override
		public FieldWriter<Object, TestIndexFieldType> getWriter() {
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
		public double getRange() {
			return 0;
		}

		@Override
		public double denormalize(
				final double value ) {
			return 0;
		}

		@Override
		public NumericRange getDenormalizedRange(
				final BinRange range ) {
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
	}

	public static class MockIndexStrategy implements
			NumericIndexStrategy
	{

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

		@Override
		public QueryRanges getQueryRanges(
				final MultiDimensionalNumericData indexedRange,
				final IndexMetaData... hints ) {
			return getQueryRanges(
					indexedRange,
					-1,
					hints);
		}

		@Override
		public QueryRanges getQueryRanges(
				final MultiDimensionalNumericData indexedRange,
				final int maxEstimatedRangeDecomposition,
				final IndexMetaData... hints ) {
			return new QueryRanges();
		}

		@Override
		public InsertionIds getInsertionIds(
				final MultiDimensionalNumericData indexedData ) {
			final List<ByteArray> ids = new ArrayList<>();
			for (final NumericData data : indexedData.getDataPerDimension()) {
				ids.add(new ByteArray(
						Double.toString(
								data.getCentroid()).getBytes()));
			}
			return new InsertionIds(
					ids);
		}

		@Override
		public InsertionIds getInsertionIds(
				final MultiDimensionalNumericData indexedData,
				final int maxEstimatedDuplicateIds ) {
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
			return new double[] {
				Integer.MAX_VALUE
			};
		}

		@Override
		public List<IndexMetaData> createMetaData() {
			return Collections.emptyList();
		}

		@Override
		public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
				final MultiDimensionalNumericData dataRange,
				final IndexMetaData... hints ) {
			final CoordinateRange[][] coordinateRangesPerDimension = new CoordinateRange[dataRange.getDimensionCount()][];
			for (int d = 0; d < coordinateRangesPerDimension.length; d++) {
				coordinateRangesPerDimension[d] = new CoordinateRange[1];
				coordinateRangesPerDimension[d][0] = new CoordinateRange(
						(long) dataRange.getMinValuesPerDimension()[0],
						(long) dataRange.getMaxValuesPerDimension()[0],
						new byte[] {});
			}
			return new MultiDimensionalCoordinateRanges[] {
				new MultiDimensionalCoordinateRanges(
						new byte[] {},
						coordinateRangesPerDimension)
			};
		}

		@Override
		public MultiDimensionalNumericData getRangeForId(
				final ByteArray partitionKey,
				final ByteArray sortKey ) {
			return null;
		}

		@Override
		public Set<ByteArray> getInsertionPartitionKeys(
				final MultiDimensionalNumericData insertionData ) {
			return null;
		}

		@Override
		public Set<ByteArray> getQueryPartitionKeys(
				final MultiDimensionalNumericData queryData,
				final IndexMetaData... hints ) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public MultiDimensionalCoordinates getCoordinatesPerDimension(
				final ByteArray partitionKey,
				final ByteArray sortKey ) {
			return new MultiDimensionalCoordinates(
					new byte[] {},
					new Coordinate[] {
						new Coordinate(
								(long) Double.parseDouble(new String(
										sortKey.getBytes())),
								new byte[] {})
					});
		}

		@Override
		public int getPartitionKeyLength() {
			return 0;
		}

		@Override
		public Set<ByteArray> getPredefinedSplits() {
			// TODO Auto-generated method stub
			return null;
		}
	}

	// *************************************************************************
	//
	// Test index model class for use in testing encoding by
	// AbstractDataAdapter.
	//
	// *************************************************************************
	public static class TestIndexModel implements
			CommonIndexModel
	{

		private final TestDimensionField[] dimensionFields;
		private String id = "testmodel";

		public TestIndexModel() {
			dimensionFields = new TestDimensionField[1];
			dimensionFields[0] = new TestDimensionField();
		}

		public TestIndexModel(
				final String id ) {
			dimensionFields = new TestDimensionField[1];
			dimensionFields[0] = new TestDimensionField();
			this.id = id;
		}

		@Override
		public FieldReader<CommonIndexValue> getReader(
				final String fieldName ) {
			final FieldReader<?> reader = dimensionFields[0].getReader();
			return (FieldReader<CommonIndexValue>) reader;
		}

		@Override
		public FieldWriter<Object, CommonIndexValue> getWriter(
				final String fieldName ) {
			final FieldWriter<?, ?> writer = dimensionFields[0].getWriter();
			return (FieldWriter<Object, CommonIndexValue>) writer;
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

		@Override
		public TestDimensionField[] getDimensions() {
			return dimensionFields;
		}

		@Override
		public String getId() {
			return id;
		}
	}

	public static class IntegerReader implements
			FieldReader<TestIndexFieldType>
	{

		@Override
		public TestIndexFieldType readField(
				final byte[] fieldData ) {
			return new TestIndexFieldType(
					Integer.parseInt(new String(
							fieldData)));
		}
	}

	public static class IntegerWriter implements
			FieldWriter<Object, TestIndexFieldType>
	{

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final String fieldName,
				final TestIndexFieldType fieldValue ) {
			return fieldValue.getVisibility();
		}

		@Override
		public byte[] writeField(
				final TestIndexFieldType fieldValue ) {
			return Integer.toString(
					fieldValue.indexValue).getBytes();
		}
	}
}
