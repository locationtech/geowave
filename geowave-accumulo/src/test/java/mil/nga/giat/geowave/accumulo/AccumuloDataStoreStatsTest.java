package mil.nga.giat.geowave.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsVisibilityHandler;
import mil.nga.giat.geowave.store.adapter.statistics.FieldTypeStatisticVisibility;
import mil.nga.giat.geowave.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.store.data.PersistentValue;
import mil.nga.giat.geowave.store.data.VisibilityWriter;
import mil.nga.giat.geowave.store.data.field.BasicReader.GeometryReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.StringReader;
import mil.nga.giat.geowave.store.data.field.BasicWriter.GeometryWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.StringWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class AccumuloDataStoreStatsTest
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStoreStatsTest.class);

	final AccumuloOptions accumuloOptions = new AccumuloOptions();

	final GeometryFactory factory = new GeometryFactory();

	AccumuloOperations accumuloOperations;

	AccumuloIndexStore indexStore;

	AccumuloAdapterStore adapterStore;

	AccumuloDataStatisticsStore statsStore;

	AccumuloDataStore mockDataStore;

	@Before
	public void setUp() {
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}
		accumuloOperations = new BasicAccumuloOperations(
				mockConnector);

		indexStore = new AccumuloIndexStore(
				accumuloOperations);

		adapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		statsStore = new AccumuloDataStatisticsStore(
				accumuloOperations);

		mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				statsStore,
				accumuloOperations,
				accumuloOptions);
	}

	public static final VisibilityWriter<TestGeometry> visWriterAAA = new VisibilityWriter<TestGeometry>() {

		@Override
		public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
				ByteArrayId fieldId ) {
			return new FieldVisibilityHandler<TestGeometry, Object>() {
				@Override
				public byte[] getVisibility(
						TestGeometry rowValue,
						ByteArrayId fieldId,
						Object fieldValue ) {
					return "aaa".getBytes();
				}

			};
		}

	};

	public static final VisibilityWriter<TestGeometry> visWriterBBB = new VisibilityWriter<TestGeometry>() {

		@Override
		public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
				ByteArrayId fieldId ) {
			return new FieldVisibilityHandler<TestGeometry, Object>() {
				@Override
				public byte[] getVisibility(
						TestGeometry rowValue,
						ByteArrayId fieldId,
						Object fieldValue ) {
					return "bbb".getBytes();
				}

			};
		}

	};

	@Test
	public void testWithOutAltIndex() {
		accumuloOptions.setCreateTable(true);
		accumuloOptions.setUseAltIndex(false);
		accumuloOptions.setPersistDataStatistics(true);
		runtest();
	}

	@Test
	public void testWithAltIndex() {
		accumuloOptions.setCreateTable(true);
		accumuloOptions.setUseAltIndex(true);
		accumuloOptions.setPersistDataStatistics(true);
		runtest();
	}

	private void runtest() {

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final Geometry testGeoFilter = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					24,
					33),
			new Coordinate(
					28,
					33),
			new Coordinate(
					28,
					31),
			new Coordinate(
					24,
					31),
			new Coordinate(
					24,
					33)
		});

		final ByteArrayId rowId0 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt"),
				visWriterAAA).get(
				0);

		final ByteArrayId rowId1 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								26,
								32)),
						"test_pt_1"),
				visWriterAAA).get(
				0);

		mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								27,
								32)),
						"test_pt_2"),
				visWriterBBB).get(
				0);

		final SpatialQuery query = new SpatialQuery(
				testGeoFilter);
		CloseableIterator it1 = mockDataStore.query(
				adapter,
				index,
				query,
				-1,
				"aaa",
				"bbb");
		int count = 0;
		while (it1.hasNext()) {
			it1.next();
			count++;
		}
		assertEquals(
				3,
				count);

		CountDataStatistics countStats = (CountDataStatistics) this.statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"aaa",
				"bbb");
		assertEquals(
				3,
				countStats.getCount());

		countStats = (CountDataStatistics) this.statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"aaa");
		assertEquals(
				2,
				countStats.getCount());

		countStats = (CountDataStatistics) this.statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"bbb");
		assertEquals(
				1,
				countStats.getCount());

		assertFalse(mockDataStore.deleteEntry(
				index,
				new ByteArrayId(
						"test_pt_2".getBytes(StringUtils.UTF8_CHAR_SET)),
				adapter.getAdapterId(),
				"aaa"));

		it1 = mockDataStore.query(
				adapter,
				index,
				query,
				-1,
				"aaa",
				"bbb");
		count = 0;
		while (it1.hasNext()) {
			it1.next();
			count++;
		}
		assertEquals(
				3,
				count);

		assertTrue(mockDataStore.deleteEntry(
				index,
				new ByteArrayId(
						"test_pt".getBytes(StringUtils.UTF8_CHAR_SET)),
				adapter.getAdapterId(),
				"aaa"));

		it1 = mockDataStore.query(
				adapter,
				index,
				query,
				-1,
				"aaa",
				"bbb");
		count = 0;
		while (it1.hasNext()) {
			it1.next();
			count++;
		}
		assertEquals(
				2,
				count);

		countStats = (CountDataStatistics) this.statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"aaa");
		assertEquals(
				1,
				countStats.getCount());

		countStats = (CountDataStatistics) this.statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"bbb");
		assertEquals(
				1,
				countStats.getCount());

		mockDataStore.deleteEntries(
				adapter,
				index,
				"aaa",
				"bbb");
		it1 = mockDataStore.query(
				adapter,
				index,
				query,
				-1,
				"aaa",
				"bbb");
		count = 0;
		while (it1.hasNext()) {
			it1.next();
			count++;
		}
		assertEquals(
				0,
				count);

		countStats = (CountDataStatistics) this.statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID);
		assertNull(countStats);

		mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								27,
								32)),
						"test_pt_2"),
				visWriterBBB).get(
				0);

		countStats = (CountDataStatistics) this.statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID, "bbb");
		assertTrue(countStats != null);

		this.statsStore.deleteObjects(adapter.getAdapterId(),"bbb");

		countStats = (CountDataStatistics) this.statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,"bbb");
		assertNull(countStats);

	}

	protected static class TestGeometry
	{
		private final Geometry geom;
		private final String id;

		public TestGeometry(
				final Geometry geom,
				final String id ) {
			this.geom = geom;
			this.id = id;
		}
	}

	protected static class TestGeometryAdapter extends
			AbstractDataAdapter<TestGeometry> implements
			StatisticalDataAdapter<TestGeometry>
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

		private final static DataStatisticsVisibilityHandler<TestGeometry> GEOMETRY_VISIBILITY_HANDLER = new FieldTypeStatisticVisibility<TestGeometry>(
				GeometryWrapper.class);
		private static final NativeFieldHandler<TestGeometry, Object> ID_FIELD_HANDLER = new NativeFieldHandler<TestGeometry, Object>() {

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
				return new GeometryReader();
			}
			else if (fieldId.equals(ID)) {
				return new StringReader();
			}
			return null;
		}

		@Override
		public FieldWriter getWriter(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return new GeometryWriter();
			}
			else if (fieldId.equals(ID)) {
				return new StringWriter();
			}
			return null;
		}

		@Override
		public DataStatistics<TestGeometry> createDataStatistics(
				final ByteArrayId statisticsId ) {
			if (BoundingBoxDataStatistics.STATS_ID.equals(statisticsId)) {
				return new GeoBoundingBoxStatistics(
						getAdapterId());
			}
			else if (CountDataStatistics.STATS_ID.equals(statisticsId)) {
				return new CountDataStatistics<TestGeometry>(
						getAdapterId());
			}
			return null;
		}

		@Override
		public DataStatisticsVisibilityHandler<TestGeometry> getVisibilityHandler(
				final ByteArrayId statisticsId ) {
			return GEOMETRY_VISIBILITY_HANDLER;
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
		public ByteArrayId[] getSupportedStatisticsIds() {
			return SUPPORTED_STATS_IDS;
		}
	}

	private final static ByteArrayId[] SUPPORTED_STATS_IDS = new ByteArrayId[] {
		BoundingBoxDataStatistics.STATS_ID,
		CountDataStatistics.STATS_ID
	};

	private static class GeoBoundingBoxStatistics extends
			BoundingBoxDataStatistics<TestGeometry>
	{

		protected GeoBoundingBoxStatistics() {
			super();
		}

		public GeoBoundingBoxStatistics(
				final ByteArrayId dataAdapterId ) {
			super(
					dataAdapterId);
		}

		@Override
		protected Envelope getEnvelope(
				final TestGeometry entry ) {
			// incorporate the bounding box of the entry's envelope
			final Geometry geometry = (Geometry) entry.geom;
			if ((geometry != null) && !geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
			return null;
		}

	}

}
