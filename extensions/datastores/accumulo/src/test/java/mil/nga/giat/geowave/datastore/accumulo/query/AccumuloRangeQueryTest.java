package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
	private PrimaryIndex index;
	private WritableDataAdapter<TestGeometry> adapter;
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
		mockDataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						mockConnector));

		index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
		adapter = new TestGeometryAdapter();

		try (IndexWriter writer = mockDataStore.createWriter(
				adapter,
				index)) {
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
		final Query intersectQuery = new SpatialQuery(
				testGeo);
		Assert.assertTrue(testdata.geom.intersects(testGeo));
		final CloseableIterator<TestGeometry> resultOfIntersect = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				intersectQuery);
		Assert.assertTrue(resultOfIntersect.hasNext());
	}

	@Test
	public void largeQuery() {
		final Geometry largeGeo = createPolygon(50000);
		final Query largeQuery = new SpatialQuery(
				largeGeo);
		final CloseableIterator itr = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				largeQuery);
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
		final Query intersectQuery = new SpatialQuery(
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
		final CloseableIterator<TestGeometry> resultOfIntersect = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				intersectQuery);
		Assert.assertFalse(resultOfIntersect.hasNext());
	}

	@Test
	public void testEncompass() {
		final Query encompassQuery = new SpatialQuery(
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
		final CloseableIterator<TestGeometry> resultOfIntersect = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				encompassQuery);
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

		final List<Coordinate> coords = new ArrayList<Coordinate>();
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

	protected WritableDataAdapter<TestGeometry> createGeometryAdapter() {
		return new TestGeometryAdapter();
	}

	protected static class TestGeometryAdapter extends
			AbstractDataAdapter<TestGeometry> implements
			StatisticsProvider<TestGeometry>
	{
		private static final ByteArrayId GEOM = new ByteArrayId(
				"myGeo");
		private static final ByteArrayId ID = new ByteArrayId(
				"myId");
		private static final PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestGeometry, CommonIndexValue, Object>() {

			@Override
			public ByteArrayId[] getNativeFieldIds() {
				return new ByteArrayId[] {
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
			public ByteArrayId getFieldId() {
				return ID;
			}

			@Override
			public Object getFieldValue(
					final TestGeometry row ) {
				return row.id;
			}

		};

		private static final List<NativeFieldHandler<TestGeometry, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<NativeFieldHandler<TestGeometry, Object>>();
		private static final List<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>>();

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
		public ByteArrayId getAdapterId() {
			return new ByteArrayId(
					"test");
		}

		@Override
		public boolean isSupported(
				final TestGeometry entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(
				final TestGeometry entry ) {
			return new ByteArrayId(
					entry.id);
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
			return null;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

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
				}

				@Override
				public TestGeometry buildRow(
						final ByteArrayId dataId ) {
					return new TestGeometry(
							geom,
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
			}
			return null;
		}

		@Override
		public ByteArrayId[] getSupportedStatisticsTypes() {
			return new ByteArrayId[0];
		}

		@Override
		public DataStatistics<TestGeometry> createDataStatistics(
				ByteArrayId statisticsId ) {
			return null;
		}

		@Override
		public EntryVisibilityHandler<TestGeometry> getVisibilityHandler(
				ByteArrayId statisticsId ) {
			return new EntryVisibilityHandler<TestGeometry>() {

				@Override
				public byte[] getVisibility(
						DataStoreEntryInfo entryInfo,
						TestGeometry entry ) {
					return null;
				}

			};
		}

	}
}
