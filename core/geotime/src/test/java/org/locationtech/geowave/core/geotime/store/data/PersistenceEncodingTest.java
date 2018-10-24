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
package org.locationtech.geowave.core.geotime.store.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.dimension.Time;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.store.adapter.AbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.DimensionMatchingIndexFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.PrimaryIndex;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class PersistenceEncodingTest
{

	private final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FLOATING));

	private static final NumericDimensionDefinition[] SPATIAL_TEMPORAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition(),
		new TimeDefinition(
				Unit.YEAR),
	};

	private static final CommonIndexModel model = new SpatialTemporalDimensionalityTypeProvider().createIndex(
			new SpatialTemporalOptions()).getIndexModel();

	private static final NumericIndexStrategy strategy = TieredSFCIndexFactory.createSingleTierStrategy(
			SPATIAL_TEMPORAL_DIMENSIONS,
			new int[] {
				16,
				16,
				16
			},
			SFCType.HILBERT);

	private static final Index index = new PrimaryIndex(
			strategy,
			model);

	Date start = null, end = null;

	@Before
	public void setUp()
			throws ParseException {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		final SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.S");
		start = dateFormat.parse("2012-04-03 13:30:23.304");
		end = dateFormat.parse("2012-04-03 14:30:23.304");
	}

	@Test
	public void testPoint() {

		final GeoObjDataAdapter adapter = new GeoObjDataAdapter(
				NATIVE_FIELD_HANDLER_LIST,
				COMMON_FIELD_HANDLER_LIST);

		final GeoObj entry = new GeoObj(
				factory.createPoint(new Coordinate(
						43.454,
						28.232)),
				start,
				end,
				"g1");
		final List<ByteArray> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index).getCompositeInsertionIds();

		assertEquals(
				1,
				ids.size());
	}

	@Test
	public void testLine() {

		final GeoObjDataAdapter adapter = new GeoObjDataAdapter(
				NATIVE_FIELD_HANDLER_LIST,
				COMMON_FIELD_HANDLER_LIST);
		final GeoObj entry = new GeoObj(
				factory.createLineString(new Coordinate[] {
					new Coordinate(
							43.444,
							28.232),
					new Coordinate(
							43.454,
							28.242)
				}),
				start,
				end,
				"g1");
		final List<ByteArray> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index).getCompositeInsertionIds();
		assertEquals(
				7,
				ids.size());

	}

	@Test
	public void testLineWithPrecisionOnTheTileEdge() {

		final NumericIndexStrategy strategy = TieredSFCIndexFactory.createSingleTierStrategy(
				SPATIAL_TEMPORAL_DIMENSIONS,
				new int[] {
					14,
					14,
					14
				},
				SFCType.HILBERT);

		final Index index = new PrimaryIndex(
				strategy,
				model);

		final GeoObjDataAdapter adapter = new GeoObjDataAdapter(
				NATIVE_FIELD_HANDLER_LIST,
				COMMON_FIELD_HANDLER_LIST);
		final GeoObj entry = new GeoObj(
				factory.createLineString(new Coordinate[] {
					new Coordinate(
							-99.22,
							33.75000000000001), // notice that
												// this gets
												// tiled as
												// 33.75
					new Coordinate(
							-99.15,
							33.75000000000001)
				// notice that this gets tiled as 33.75
						}),
				new Date(
						352771200000l),
				new Date(
						352771200000l),
				"g1");
		final List<ByteArray> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index).getCompositeInsertionIds();
		assertEquals(
				4,
				ids.size());
	}

	@Test
	public void testPoly() {
		final GeoObjDataAdapter adapter = new GeoObjDataAdapter(
				NATIVE_FIELD_HANDLER_LIST,
				COMMON_FIELD_HANDLER_LIST);
		final GeoObj entry = new GeoObj(
				factory.createLineString(new Coordinate[] {
					new Coordinate(
							43.444,
							28.232),
					new Coordinate(
							43.454,
							28.242),
					new Coordinate(
							43.444,
							28.252),
					new Coordinate(
							43.444,
							28.232),
				}),
				start,
				end,
				"g1");
		final List<ByteArray> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index).getCompositeInsertionIds();
		assertEquals(
				18,
				ids.size());
	}

	@Test
	public void testPointRange() {

		final GeoObjDataAdapter adapter = new GeoObjDataAdapter(
				NATIVE_FIELD_RANGE_HANDLER_LIST,
				COMMON_FIELD_RANGE_HANDLER_LIST);

		final GeoObj entry = new GeoObj(
				factory.createPoint(new Coordinate(
						43.454,
						28.232)),
				start,
				end,
				"g1");
		final List<ByteArray> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index).getCompositeInsertionIds();

		assertEquals(
				8,
				ids.size());
	}

	@Test
	public void testLineRnge() {

		final GeoObjDataAdapter adapter = new GeoObjDataAdapter(
				NATIVE_FIELD_RANGE_HANDLER_LIST,
				COMMON_FIELD_RANGE_HANDLER_LIST);
		final GeoObj entry = new GeoObj(
				factory.createLineString(new Coordinate[] {
					new Coordinate(
							43.444,
							28.232),
					new Coordinate(
							43.454,
							28.242)
				}),
				start,
				end,
				"g1");
		final List<ByteArray> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index).getCompositeInsertionIds();
		assertTrue(ids.size() < 100);
	}

	private static final String GEOM = "myGeo";
	private static final String ID = "myId";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";

	private static final List<NativeFieldHandler<GeoObj, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<>();
	private static final List<NativeFieldHandler<GeoObj, Object>> NATIVE_FIELD_RANGE_HANDLER_LIST = new ArrayList<>();
	private static final List<PersistentIndexFieldHandler<GeoObj, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<>();
	private static final List<PersistentIndexFieldHandler<GeoObj, ? extends CommonIndexValue, Object>> COMMON_FIELD_RANGE_HANDLER_LIST = new ArrayList<>();

	private static final NativeFieldHandler<GeoObj, Object> END_TIME_FIELD_HANDLER = new NativeFieldHandler<GeoObj, Object>() {

		@Override
		public String getFieldName() {
			return END_TIME;
		}

		@Override
		public Object getFieldValue(
				final GeoObj row ) {
			return row.endTime;
		}

	};

	private static final NativeFieldHandler<GeoObj, Object> ID_FIELD_HANDLER = new NativeFieldHandler<GeoObj, Object>() {

		@Override
		public String getFieldName() {
			return ID;
		}

		@Override
		public Object getFieldValue(
				final GeoObj row ) {
			return row.id;
		}

	};

	private static final PersistentIndexFieldHandler<GeoObj, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<GeoObj, CommonIndexValue, Object>() {

		@Override
		public String[] getNativeFieldNames() {
			return new String[] {
				GEOM
			};
		}

		@Override
		public CommonIndexValue toIndexValue(
				final GeoObj row ) {
			return new GeometryWrapper(
					row.geometry,
					new byte[0]);
		}

		@Override
		public PersistentValue<Object>[] toNativeValues(
				final CommonIndexValue indexValue ) {
			return new PersistentValue[] {
				new PersistentValue<Object>(
						GEOM,
						((GeometryWrapper) indexValue).getGeometry())
			};
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {

		}
	};

	static {
		COMMON_FIELD_HANDLER_LIST.add(GEOM_FIELD_HANDLER);
		COMMON_FIELD_HANDLER_LIST.add(new TimeFieldHandler());
		COMMON_FIELD_RANGE_HANDLER_LIST.add(GEOM_FIELD_HANDLER);
		COMMON_FIELD_RANGE_HANDLER_LIST.add(new TimeRangeFieldHandler());
		NATIVE_FIELD_HANDLER_LIST.add(ID_FIELD_HANDLER);
		NATIVE_FIELD_HANDLER_LIST.add(END_TIME_FIELD_HANDLER);
		NATIVE_FIELD_RANGE_HANDLER_LIST.add(ID_FIELD_HANDLER);
	}

	public static class GeoObjDataAdapter extends
			AbstractDataAdapter<GeoObj>
	{
		public GeoObjDataAdapter() {
			super();
		}

		public GeoObjDataAdapter(
				final List<NativeFieldHandler<GeoObj, Object>> nativeFields,
				final List<PersistentIndexFieldHandler<GeoObj, ? extends CommonIndexValue, Object>> commonFields ) {
			super(
					commonFields,
					nativeFields);
		}

		@Override
		public String getTypeName() {
			return "geoobj";
		}

		@Override
		public ByteArray getDataId(
				final GeoObj entry ) {
			return new ByteArray(
					entry.id.getBytes());
		}

		@Override
		public FieldReader getReader(
				final String fieldId ) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultReaderForClass(Geometry.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultReaderForClass(String.class);
			}
			else if (fieldId.equals(START_TIME)) {
				return FieldUtils.getDefaultReaderForClass(Date.class);
			}
			else if (fieldId.equals(END_TIME)) {
				return FieldUtils.getDefaultReaderForClass(Date.class);
			}
			return null;
		}

		@Override
		public FieldWriter getWriter(
				final String fieldId ) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultWriterForClass(Geometry.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultWriterForClass(String.class);
			}
			else if (fieldId.equals(START_TIME)) {
				return FieldUtils.getDefaultWriterForClass(Date.class);
			}
			else if (fieldId.equals(END_TIME)) {
				return FieldUtils.getDefaultWriterForClass(Date.class);
			}
			return null;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<GeoObj, Object>() {
				private String id;
				private Geometry geom;
				private Date stime;
				private Date etime;

				@Override
				public void setField(
						final String id,
						final Object fieldValue ) {
					if (id.equals(GEOM)) {
						geom = (Geometry) fieldValue;
					}
					else if (id.equals(ID)) {
						this.id = (String) fieldValue;
					}
					else if (id.equals(START_TIME)) {
						stime = (Date) fieldValue;
					}
					else {
						etime = (Date) fieldValue;
					}
				}

				@Override
				public void setFields(
						final Map<String, Object> values ) {
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
				public GeoObj buildRow(
						final ByteArray dataId ) {
					return new GeoObj(
							geom,
							stime,
							etime,
							id);
				}
			};
		}

		@Override
		public int getPositionOfOrderedField(
				final CommonIndexModel model,
				final String fieldId ) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (fieldId.equals(dimensionField.getFieldName())) {
					return i;
				}
				i++;
			}
			if (fieldId.equals(GEOM)) {
				return i;
			}
			else if (fieldId.equals(ID)) {
				return i + 1;
			}
			else if (fieldId.equals(START_TIME)) {
				return i + 2;
			}
			else if (fieldId.equals(END_TIME)) {
				return i + 3;
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
					return GEOM;
				}
				else if (position == (numDimensions + 1)) {
					return ID;
				}
				else if (position == (numDimensions + 2)) {
					return START_TIME;
				}
				else if (position == (numDimensions + 3)) {
					return END_TIME;
				}
			}
			return null;
		}
	}

	private static class GeoObj
	{
		private final Geometry geometry;
		private final String id;
		private final Date startTime;
		private final Date endTime;

		public GeoObj(
				final Geometry geometry,
				final Date startTime,
				final Date endTime,
				final String id ) {
			super();
			this.geometry = geometry;
			this.startTime = startTime;
			this.endTime = endTime;
			this.id = id;
		}

	}

	public static class TimeFieldHandler implements
			PersistentIndexFieldHandler<GeoObj, CommonIndexValue, Object>,
			DimensionMatchingIndexFieldHandler<GeoObj, CommonIndexValue, Object>
	{

		public TimeFieldHandler() {}

		@Override
		public String[] getNativeFieldNames() {
			return new String[] {
				START_TIME
			};
		}

		@Override
		public CommonIndexValue toIndexValue(
				final GeoObj row ) {
			return new Time.Timestamp(
					row.startTime.getTime(),
					new byte[0]);
		}

		@Override
		public PersistentValue<Object>[] toNativeValues(
				final CommonIndexValue indexValue ) {
			return new PersistentValue[] {
				new PersistentValue<Object>(
						START_TIME,
						new Date(
								(long) ((Time.TimeRange) indexValue).toNumericData().getMin())),
				new PersistentValue<Object>(
						END_TIME,
						new Date(
								(long) ((Time.TimeRange) indexValue).toNumericData().getMin()))
			};
		}

		@Override
		public String[] getSupportedIndexFieldNames() {
			return new String[] {
				new TimeField(
						Unit.YEAR).getFieldName()
			};
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {

		}
	}

	public static class TimeRangeFieldHandler implements
			PersistentIndexFieldHandler<GeoObj, CommonIndexValue, Object>,
			DimensionMatchingIndexFieldHandler<GeoObj, CommonIndexValue, Object>
	{

		public TimeRangeFieldHandler() {}

		@Override
		public String[] getNativeFieldNames() {
			return new String[] {
				START_TIME,
				END_TIME
			};
		}

		@Override
		public CommonIndexValue toIndexValue(
				final GeoObj row ) {
			return new Time.TimeRange(
					row.startTime.getTime(),
					row.endTime.getTime(),
					new byte[0]);
		}

		@Override
		public PersistentValue<Object>[] toNativeValues(
				final CommonIndexValue indexValue ) {
			return new PersistentValue[] {
				new PersistentValue<Object>(
						START_TIME,
						new Date(
								(long) ((Time.TimeRange) indexValue).toNumericData().getMin())),
				new PersistentValue<Object>(
						END_TIME,
						new Date(
								(long) ((Time.TimeRange) indexValue).toNumericData().getMin()))
			};
		}

		@Override
		public String[] getSupportedIndexFieldNames() {
			return new String[] {
				new TimeField(
						Unit.YEAR).getFieldName()
			};
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {

		}
	}
}
