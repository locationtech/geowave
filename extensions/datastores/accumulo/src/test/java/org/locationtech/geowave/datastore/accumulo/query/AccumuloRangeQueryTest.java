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
package org.locationtech.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.AbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStore;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

public class AccumuloRangeQueryTest
{
	private DataStore mockDataStore;
	private Index index;
	private DataTypeAdapter<TestGeometry> adapter;
	private final GeometryFactory factory = new GeometryFactory();
	private final TestGeometry testdata = new TestGeometry(
			factory.createPolygon(new Coordinate[] {
				new Coordinate(
						1.025,
						1.032),
				new Coordinate(
						1.026,
						1.032),
				new Coordinate(
						1.026,
						1.033),
				new Coordinate(
						1.025,
						1.032)
			}),
			"test_shape_1");

	@Before
	public void ingestGeometries()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		final MockInstance mockInstance = new MockInstance();
		final Connector mockConnector = mockInstance.getConnector(
				"root",
				new PasswordToken(
						new byte[0]));

		final AccumuloOptions options = new AccumuloOptions();
		mockDataStore = new AccumuloDataStore(
				new AccumuloOperations(
						mockConnector,
						options),
				options);

		index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		adapter = new TestGeometryAdapter();
		mockDataStore.addType(
				adapter,
				index);
		try (Writer writer = mockDataStore.createWriter(adapter.getTypeName())) {
			writer.write(testdata);
		}

	}

	@Test
	public void testIntersection() {
		final Geometry testGeo = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					1.0249,
					1.0319),
			new Coordinate(
					1.0261,
					1.0319),
			new Coordinate(
					1.0261,
					1.0323),
			new Coordinate(
					1.0249,
					1.0319)
		});
		final QueryConstraints intersectQuery = new SpatialQuery(
				testGeo);
		Assert.assertTrue(testdata.geom.intersects(testGeo));
		final CloseableIterator<TestGeometry> resultOfIntersect = (CloseableIterator) mockDataStore.query(QueryBuilder
				.newBuilder()
				.addTypeName(
						adapter.getTypeName())
				.indexName(
						index.getName())
				.constraints(
						intersectQuery)
				.build());
		Assert.assertTrue(resultOfIntersect.hasNext());
	}

	@Test
	public void largeQuery() {
		final Geometry largeGeo = createPolygon(50000);
		final QueryConstraints largeQuery = new SpatialQuery(
				largeGeo);
		final CloseableIterator itr = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).constraints(
				largeQuery).build());
		int numfeats = 0;
		while (itr.hasNext()) {
			itr.next();
			numfeats++;
		}
		Assert.assertEquals(
				numfeats,
				1);
	}

	/**
	 * Verifies equality for interning is still working as expected
	 * (topologically), as the the largeQuery() test has a dependency on this;
	 *
	 * @throws ParseException
	 */
	@Test
	public void testInterning()
			throws ParseException {
		final Geometry g = GeometryUtils.GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
			new Coordinate(
					0,
					0),
			new Coordinate(
					1,
					0),
			new Coordinate(
					1,
					1),
			new Coordinate(
					0,
					1),
			new Coordinate(
					0,
					0)
		});
		final Geometry gNewInstance = GeometryUtils.GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
			new Coordinate(
					0,
					0),
			new Coordinate(
					1,
					0),
			new Coordinate(
					1,
					1),
			new Coordinate(
					0,
					1),
			new Coordinate(
					0,
					0)
		});
		final WKBWriter wkbWriter = new WKBWriter();
		final byte[] b = wkbWriter.write(g);
		final byte[] b2 = new byte[b.length];
		System.arraycopy(
				b,
				0,
				b2,
				0,
				b.length);
		final WKBReader wkbReader = new WKBReader();
		final Geometry gSerialized = wkbReader.read(b);
		final Geometry gSerializedArrayCopy = wkbReader.read(b2);

		Assert.assertEquals(
				g,
				gNewInstance);
		Assert.assertEquals(
				g,
				gSerializedArrayCopy);
		Assert.assertEquals(
				gSerialized,
				gSerializedArrayCopy);
		Assert.assertEquals(
				gSerialized,
				gSerializedArrayCopy);
	}

	@Test
	public void testMiss() {
		final QueryConstraints intersectQuery = new SpatialQuery(
				factory.createPolygon(new Coordinate[] {
					new Coordinate(
							1.0247,
							1.0319),
					new Coordinate(
							1.0249,
							1.0319),
					new Coordinate(
							1.0249,
							1.0323),
					new Coordinate(
							1.0247,
							1.0319)
				}));
		final CloseableIterator<TestGeometry> resultOfIntersect = (CloseableIterator) mockDataStore.query(QueryBuilder
				.newBuilder()
				.addTypeName(
						adapter.getTypeName())
				.indexName(
						index.getName())
				.constraints(
						intersectQuery)
				.build());
		Assert.assertFalse(resultOfIntersect.hasNext());
	}

	@Test
	public void testEncompass() {
		final QueryConstraints encompassQuery = new SpatialQuery(
				factory.createPolygon(new Coordinate[] {
					new Coordinate(
							1.0249,
							1.0319),
					new Coordinate(
							1.0261,
							1.0319),
					new Coordinate(
							1.0261,
							1.0331),
					new Coordinate(
							1.0249,
							1.0319)
				}));
		final CloseableIterator<TestGeometry> resultOfIntersect = (CloseableIterator) mockDataStore.query(QueryBuilder
				.newBuilder()
				.addTypeName(
						adapter.getTypeName())
				.indexName(
						index.getName())
				.constraints(
						encompassQuery)
				.build());
		Assert.assertTrue(resultOfIntersect.hasNext());
		final TestGeometry geom1 = resultOfIntersect.next();
		Assert.assertEquals(
				"test_shape_1",
				geom1.id);
	}

	private static Polygon createPolygon(
			final int numPoints ) {
		final double centerX = 4;
		final double centerY = 12;
		final int maxRadius = 80;

		final List<Coordinate> coords = new ArrayList<>();
		final Random rand = new Random(
				8675309l);

		final double increment = (double) 360 / numPoints;

		for (double theta = 0; theta <= 360; theta += increment) {
			final double radius = (rand.nextDouble() * maxRadius) + 0.1;
			final double rad = (theta * Math.PI) / 180.0;
			final double x = centerX + (radius * Math.sin(rad));
			final double y = centerY + (radius * Math.cos(rad));
			coords.add(new Coordinate(
					x,
					y));
		}
		coords.add(coords.get(0));
		return GeometryUtils.GEOMETRY_FACTORY.createPolygon(coords.toArray(new Coordinate[coords.size()]));
	}

	protected static class TestGeometry
	{
		protected final Geometry geom;
		protected final String id;

		public TestGeometry(
				final Geometry geom,
				final String id ) {
			this.geom = geom;
			this.id = id;
		}
	}

	protected DataTypeAdapter<TestGeometry> createGeometryAdapter() {
		return new TestGeometryAdapter();
	}

	public static class TestGeometryAdapter extends
			AbstractDataAdapter<TestGeometry> implements
			StatisticsProvider<TestGeometry>
	{
		private static final String GEOM = "myGeo";
		private static final String ID = "myId";
		private static final PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestGeometry, CommonIndexValue, Object>() {

			@Override
			public String[] getNativeFieldNames() {
				return new String[] {
					GEOM
				};
			}

			@Override
			public CommonIndexValue toIndexValue(
					final TestGeometry row ) {
				return new GeometryWrapper(
						row.geom,
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
		private static final NativeFieldHandler<TestGeometry, Object> ID_FIELD_HANDLER = new NativeFieldHandler<AccumuloRangeQueryTest.TestGeometry, Object>() {

			@Override
			public String getFieldName() {
				return ID;
			}

			@Override
			public Object getFieldValue(
					final TestGeometry row ) {
				return row.id;
			}

		};

		private static final List<NativeFieldHandler<TestGeometry, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<>();
		private static final List<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<>();

		static {
			COMMON_FIELD_HANDLER_LIST.add(GEOM_FIELD_HANDLER);
			NATIVE_FIELD_HANDLER_LIST.add(ID_FIELD_HANDLER);
		}

		public TestGeometryAdapter() {
			super(
					COMMON_FIELD_HANDLER_LIST,
					NATIVE_FIELD_HANDLER_LIST);
		}

		@Override
		public String getTypeName() {
			return "test";
		}

		@Override
		public ByteArray getDataId(
				final TestGeometry entry ) {
			return new ByteArray(
					entry.id);
		}

		@Override
		public FieldReader getReader(
				final String fieldName ) {
			if (fieldName.equals(GEOM)) {
				return FieldUtils.getDefaultReaderForClass(Geometry.class);
			}
			else if (fieldName.equals(ID)) {
				return FieldUtils.getDefaultReaderForClass(String.class);
			}
			return null;
		}

		@Override
		public FieldWriter getWriter(
				final String fieldName ) {
			if (fieldName.equals(GEOM)) {
				return FieldUtils.getDefaultWriterForClass(Geometry.class);
			}
			else if (fieldName.equals(ID)) {
				return FieldUtils.getDefaultWriterForClass(String.class);
			}
			return null;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

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
				}

				@Override
				public TestGeometry buildRow(
						final ByteArray dataId ) {
					return new TestGeometry(
							geom,
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
			}
			return null;
		}

		@Override
		public StatisticsId[] getSupportedStatistics() {
			return new StatisticsId[0];
		}

		@Override
		public InternalDataStatistics<TestGeometry, ?, ?> createDataStatistics(
				final StatisticsId statisticsId ) {
			return null;
		}

		@Override
		public EntryVisibilityHandler<TestGeometry> getVisibilityHandler(
				final CommonIndexModel indexModel,
				final DataTypeAdapter<TestGeometry> adapter,
				final StatisticsId statisticsId ) {
			// TODO Auto-generated method stub
			return new EntryVisibilityHandler<AccumuloRangeQueryTest.TestGeometry>() {

				@Override
				public byte[] getVisibility(
						final TestGeometry entry,
						final GeoWaveRow... kvs ) {
					return null;
				}
			};
		}
	}
}
