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
package mil.nga.giat.geowave.core.geotime.store.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalOptions;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.DimensionMatchingIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.junit.Before;
import org.junit.Test;

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

	private static final CommonIndexModel model = new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex(
			new SpatialTemporalOptions()).getIndexModel();

	private static final NumericIndexStrategy strategy = TieredSFCIndexFactory.createSingleTierStrategy(
			SPATIAL_TEMPORAL_DIMENSIONS,
			new int[] {
				16,
				16,
				16
			},
			SFCType.HILBERT);

	private static final PrimaryIndex index = new PrimaryIndex(
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
		final List<ByteArrayId> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index);

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
		final List<ByteArrayId> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index);
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

		final PrimaryIndex index = new PrimaryIndex(
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
		final List<ByteArrayId> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index);
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
		final List<ByteArrayId> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index);
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
		final List<ByteArrayId> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index);

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
		final List<ByteArrayId> ids = adapter.encode(
				entry,
				model).getInsertionIds(
				index);
		assertTrue(ids.size() < 100);
	}

	private static final ByteArrayId GEOM = new ByteArrayId(
			"myGeo");
	private static final ByteArrayId ID = new ByteArrayId(
			"myId");
	private static final ByteArrayId START_TIME = new ByteArrayId(
			"startTime");
	private static final ByteArrayId END_TIME = new ByteArrayId(
			"endTime");

	private static final List<NativeFieldHandler<GeoObj, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<NativeFieldHandler<GeoObj, Object>>();
	private static final List<NativeFieldHandler<GeoObj, Object>> NATIVE_FIELD_RANGE_HANDLER_LIST = new ArrayList<NativeFieldHandler<GeoObj, Object>>();
	private static final List<PersistentIndexFieldHandler<GeoObj, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<PersistentIndexFieldHandler<GeoObj, ? extends CommonIndexValue, Object>>();
	private static final List<PersistentIndexFieldHandler<GeoObj, ? extends CommonIndexValue, Object>> COMMON_FIELD_RANGE_HANDLER_LIST = new ArrayList<PersistentIndexFieldHandler<GeoObj, ? extends CommonIndexValue, Object>>();

	private static final NativeFieldHandler<GeoObj, Object> END_TIME_FIELD_HANDLER = new NativeFieldHandler<GeoObj, Object>() {

		@Override
		public ByteArrayId getFieldId() {
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
		public ByteArrayId getFieldId() {
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
		public ByteArrayId[] getNativeFieldIds() {
			return new ByteArrayId[] {
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
		public ByteArrayId getAdapterId() {
			return new ByteArrayId(
					"geoobj".getBytes());
		}

		@Override
		public boolean isSupported(
				final GeoObj entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(
				final GeoObj entry ) {
			return new ByteArrayId(
					entry.id.getBytes());
		}

		@Override
		public FieldReader getReader(
				final ByteArrayId fieldId ) {
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
				final ByteArrayId fieldId ) {
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
						final PersistentValue<Object> fieldValue ) {
					if (fieldValue.getId().equals(
							GEOM)) {
						geom = (Geometry) fieldValue.getValue();
					}
					else if (fieldValue.getId().equals(
							ID)) {
						id = (String) fieldValue.getValue();
					}
					else if (fieldValue.getId().equals(
							START_TIME)) {
						stime = (Date) fieldValue.getValue();
					}
					else {
						etime = (Date) fieldValue.getValue();
					}
				}

				@Override
				public GeoObj buildRow(
						final ByteArrayId dataId ) {
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
				final ByteArrayId fieldId ) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (fieldId.equals(dimensionField.getFieldId())) {
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
		public ByteArrayId getFieldIdForPosition(
				final CommonIndexModel model,
				final int position ) {
			if (position < model.getDimensions().length) {
				int i = 0;
				for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
					if (i == position) {
						return dimensionField.getFieldId();
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

		@Override
		public void init(
				PrimaryIndex... indices ) {
			// TODO Auto-generated method stub

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
		public ByteArrayId[] getNativeFieldIds() {
			return new ByteArrayId[] {
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
		public ByteArrayId[] getSupportedIndexFieldIds() {
			return new ByteArrayId[] {
				new TimeField(
						Unit.YEAR).getFieldId()
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
		public ByteArrayId[] getNativeFieldIds() {
			return new ByteArrayId[] {
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
		public ByteArrayId[] getSupportedIndexFieldIds() {
			return new ByteArrayId[] {
				new TimeField(
						Unit.YEAR).getFieldId()
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
