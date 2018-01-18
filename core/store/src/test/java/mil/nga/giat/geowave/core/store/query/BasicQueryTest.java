/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public class BasicQueryTest
{

	final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ssz");

	@Test
	public void testIsSupported() {
		final ConstraintSet cs1 = new ConstraintSet();
		final PrimaryIndex index = new CustomIdIndex(
				new ExampleNumericIndexStrategy(),
				new BasicIndexModel(
						new NumericDimensionField[] {
							new ExampleDimensionOne(),
							new ExampleDimensionTwo()
						}),
				new ByteArrayId(
						"22"));
		assertTrue(cs1.isSupported(index));
		cs1.addConstraint(
				ExampleDimensionOne.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						true));
		cs1.addConstraint(
				ExampleDimensionTwo.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						true));
		assertTrue(cs1.isSupported(index));
		cs1.addConstraint(
				ExampleDimensionThree.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						true));
		assertTrue(cs1.isSupported(index));
		final ConstraintSet cs2 = new ConstraintSet();
		cs2.addConstraint(
				ExampleDimensionThree.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						false));
		assertFalse(cs2.isSupported(index));
	}

	@Test
	public void testIntersectCasesWithPersistence() {
		final PrimaryIndex index = new CustomIdIndex(
				new ExampleNumericIndexStrategy(),
				new BasicIndexModel(
						new NumericDimensionField[] {
							new ExampleDimensionOne(),
							new ExampleDimensionTwo()
						}),
				new ByteArrayId(
						"22"));
		final List<MultiDimensionalNumericData> expectedResults = new ArrayList<>();
		expectedResults.add(new BasicNumericDataset(
				new NumericData[] {
					new ConstrainedIndexValue(
							0.3,
							0.5),
					new ConstrainedIndexValue(
							0.1,
							0.7)
				}));

		final ConstraintSet cs1 = new ConstraintSet();
		cs1.addConstraint(
				ExampleDimensionOne.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						true));

		cs1.addConstraint(
				ExampleDimensionTwo.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.4,
								0.7),
						true));

		final ConstraintSet cs2a = new ConstraintSet();
		cs2a.addConstraint(
				ExampleDimensionTwo.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.1,
								0.2),
						true));

		final Constraints constraints = new Constraints(
				Arrays.asList(cs2a)).merge(Collections.singletonList(cs1));

		assertEquals(
				expectedResults,
				constraints.getIndexConstraints(new ExampleNumericIndexStrategy()));

		final byte[] image = new BasicQuery(
				constraints).toBinary();
		final BasicQuery query = new BasicQuery();
		query.fromBinary(image);

		assertEquals(
				expectedResults,
				query.getIndexConstraints(index));

	}

	@Test
	public void testDisjointCasesWithPersistence() {

		final List<MultiDimensionalNumericData> expectedResults = new ArrayList<>();
		expectedResults.add(new BasicNumericDataset(
				new NumericData[] {
					new ConstrainedIndexValue(
							0.3,
							0.7),
					new ConstrainedIndexValue(
							0.1,
							2.3)
				}));
		expectedResults.add(new BasicNumericDataset(
				new NumericData[] {
					new ConstrainedIndexValue(
							0.3,
							0.7),
					new ConstrainedIndexValue(
							3.4,
							3.7)
				}));

		final ConstraintSet cs1 = new ConstraintSet();
		cs1.addConstraint(
				ExampleDimensionOne.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						true));

		cs1.addConstraint(
				ExampleDimensionOne.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.4,
								0.7),
						true));

		final ConstraintSet cs2a = new ConstraintSet();
		cs2a.addConstraint(
				ExampleDimensionTwo.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.1,
								0.2),
						true));

		cs2a.addConstraint(
				ExampleDimensionTwo.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								2.1,
								2.3),
						true));

		final ConstraintSet cs2b = new ConstraintSet();
		cs2b.addConstraint(
				ExampleDimensionTwo.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								3.4,
								3.7),
						true));

		final Constraints constraints = new Constraints(
				Arrays.asList(
						cs2a,
						cs2b)).merge(Collections.singletonList(cs1));

		assertEquals(
				expectedResults,
				constraints.getIndexConstraints(new ExampleNumericIndexStrategy()));

		final byte[] image = new BasicQuery(
				constraints).toBinary();
		final BasicQuery query = new BasicQuery();
		query.fromBinary(image);
		final PrimaryIndex index = new CustomIdIndex(
				new ExampleNumericIndexStrategy(),
				new BasicIndexModel(
						new NumericDimensionField[] {
							new ExampleDimensionOne(),
							new ExampleDimensionTwo()
						}),
				new ByteArrayId(
						"22"));
		assertEquals(
				expectedResults,
				query.getIndexConstraints(index));

		final List<QueryFilter> filters = query.createFilters(index);

		assertEquals(
				1,
				filters.size());

		final Map<ByteArrayId, ConstrainedIndexValue> fieldIdToValueMap = new HashMap<>();
		fieldIdToValueMap.put(
				new ByteArrayId(
						"one"),
				new ConstrainedIndexValue(
						0.4,
						0.4));
		fieldIdToValueMap.put(
				new ByteArrayId(
						"two"),
				new ConstrainedIndexValue(
						0.5,
						0.5));

		final CommonIndexModel model = null;
		assertTrue(filters.get(
				0).accept(
				model,
				new CommonIndexedPersistenceEncoding(
						new ByteArrayId(
								"adapter"),
						new ByteArrayId(
								"data"),
						new ByteArrayId(
								"index"),
						1, // duplicate count
						new PersistentDataset(
								fieldIdToValueMap),
						null)));
		fieldIdToValueMap.put(
				new ByteArrayId(
						"one"),
				new ConstrainedIndexValue(
						0.1,
						0.1));
		assertFalse(filters.get(
				0).accept(
				model,
				new CommonIndexedPersistenceEncoding(
						new ByteArrayId(
								"adapter"),
						new ByteArrayId(
								"data"),
						new ByteArrayId(
								"index"),
						1, // duplicate count
						new PersistentDataset(
								fieldIdToValueMap),
						null)));

		fieldIdToValueMap.put(
				new ByteArrayId(
						"one"),
				new ConstrainedIndexValue(
						0.4,
						0.4));
		fieldIdToValueMap.put(
				new ByteArrayId(
						"two"),
				new ConstrainedIndexValue(
						5.0,
						5.0));
		assertFalse(filters.get(
				0).accept(
				model,
				new CommonIndexedPersistenceEncoding(
						new ByteArrayId(
								"adapter"),
						new ByteArrayId(
								"data"),
						new ByteArrayId(
								"index"),
						1, // duplicate count
						new PersistentDataset(
								fieldIdToValueMap),
						null)));

		/**
		 * Tests the 'OR' Case
		 */
		fieldIdToValueMap.put(
				new ByteArrayId(
						"two"),
				new ConstrainedIndexValue(
						3.5,
						3.5));
		assertTrue(filters.get(
				0).accept(
				model,
				new CommonIndexedPersistenceEncoding(
						new ByteArrayId(
								"adapter"),
						new ByteArrayId(
								"data"),
						new ByteArrayId(
								"index"),
						1, // duplicate count
						new PersistentDataset(
								fieldIdToValueMap),
						null)));

	}

	public static class ExampleNumericIndexStrategy implements
			NumericIndexStrategy
	{

		@Override
		public byte[] toBinary() {
			return null;
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

		@Override
		public List<ByteArrayRange> getQueryRanges(
				final MultiDimensionalNumericData indexedRange,
				final IndexMetaData... hints ) {
			return null;
		}

		@Override
		public List<ByteArrayRange> getQueryRanges(
				final MultiDimensionalNumericData indexedRange,
				final int maxEstimatedRangeDecomposition,
				final IndexMetaData... hints ) {
			return null;
		}

		@Override
		public List<ByteArrayId> getInsertionIds(
				final MultiDimensionalNumericData indexedData ) {
			return null;
		}

		@Override
		public List<ByteArrayId> getInsertionIds(
				final MultiDimensionalNumericData indexedData,
				final int maxEstimatedDuplicateIds ) {
			return null;
		}

		@Override
		public MultiDimensionalNumericData getRangeForId(
				final ByteArrayId insertionId ) {
			return null;
		}

		@Override
		public MultiDimensionalCoordinates getCoordinatesPerDimension(
				final ByteArrayId insertionId ) {
			return null;
		}

		@Override
		public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
			return new NumericDimensionDefinition[] {
				new ExampleDimensionOne(),
				new ExampleDimensionTwo()
			};
		}

		@Override
		public String getId() {
			return "test-bqt";
		}

		@Override
		public double[] getHighestPrecisionIdRangePerDimension() {
			return null;
		}

		@Override
		public Set<ByteArrayId> getNaturalSplits() {
			return null;
		}

		@Override
		public int getByteOffsetFromDimensionalIndex() {
			return 0;
		}

		@Override
		public List<IndexMetaData> createMetaData() {
			return Collections.emptyList();
		}

		@Override
		public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
				final MultiDimensionalNumericData dataRange,
				final IndexMetaData... hints ) {
			return null;
		}
	}

	public static class ConstrainedIndexValue extends
			NumericRange implements
			CommonIndexValue
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		public ConstrainedIndexValue(
				final double min,
				final double max ) {
			super(
					min,
					max);
			//
		}

		@Override
		public byte[] getVisibility() {
			return new byte[0];
		}

		@Override
		public void setVisibility(
				final byte[] visibility ) {

		}

		@Override
		public boolean overlaps(
				final NumericDimensionField[] field,
				final NumericData[] rangeData ) {
			return false;
		}

	}

	public static class ExampleDimensionOne implements
			NumericDimensionField<ConstrainedIndexValue>
	{

		public ExampleDimensionOne() {

		}

		@Override
		public double getRange() {
			return 10;
		}

		@Override
		public double normalize(
				final double value ) {
			return value;
		}

		@Override
		public double denormalize(
				final double value ) {
			return value;
		}

		@Override
		public BinRange[] getNormalizedRanges(
				final NumericData range ) {
			return new BinRange[] {
				new BinRange(
						range.getMin(),
						range.getMax())
			};
		}

		@Override
		public NumericRange getDenormalizedRange(
				final BinRange range ) {
			return new NumericRange(
					range.getNormalizedMin(),
					range.getNormalizedMax());
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
			return new NumericRange(
					0,
					10);
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
				final ConstrainedIndexValue dataElement ) {
			return dataElement;
		}

		@Override
		public ByteArrayId getFieldId() {
			return new ByteArrayId(
					"one");
		}

		@Override
		public FieldWriter<?, ConstrainedIndexValue> getWriter() {
			return null;
		}

		@Override
		public FieldReader<ConstrainedIndexValue> getReader() {
			return null;
		}

		@Override
		public NumericDimensionDefinition getBaseDefinition() {
			return this;
		}
	}

	public static class ExampleDimensionTwo extends
			ExampleDimensionOne
	{

		public ExampleDimensionTwo() {
			super();
		}

		@Override
		public ByteArrayId getFieldId() {
			return new ByteArrayId(
					"two");
		}

	}

	public static class ExampleDimensionThree extends
			ExampleDimensionOne
	{

		public ExampleDimensionThree() {
			super();
		}

		@Override
		public ByteArrayId getFieldId() {
			return new ByteArrayId(
					"three");
		}

	}
}
