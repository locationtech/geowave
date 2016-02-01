package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometry;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometryAdapter;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class DeleteWriterTest
{
	private final MockInstance mockInstance = new MockInstance();
	private Connector mockConnector = null;
	private BasicAccumuloOperations operations;
	private DataStore mockDataStore;
	private List<ByteArrayId> rowId1s;
	private List<ByteArrayId> rowId2s;
	private WritableDataAdapter<AccumuloDataStoreStatsTest.TestGeometry> adapter;
	private DataStatisticsStore statsStore;

	private static final CommonIndexModel model = new SpatialDimensionalityTypeProvider().createPrimaryIndex().getIndexModel();

	private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition()
	};

	private static final NumericIndexStrategy strategy = TieredSFCIndexFactory.createSingleTierStrategy(
			SPATIAL_DIMENSIONS,
			new int[] {
				16,
				16
			},
			SFCType.HILBERT);

	final SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.S");

	private static final PrimaryIndex index = new PrimaryIndex(
			strategy,
			model);

	@Before
	public void setUp()
			throws IOException {

		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));

			operations = new BasicAccumuloOperations(
					mockConnector);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			e.printStackTrace();
		}

		operations.createTable("test_table");
		mockDataStore = new AccumuloDataStore(
				operations);

		statsStore = new AccumuloDataStatisticsStore(
				operations);

		adapter = new TestGeometryAdapter();
		final GeometryFactory factory = new GeometryFactory();

		try (IndexWriter indexWriter = mockDataStore.createIndexWriter(
				index,
				DataStoreUtils.DEFAULT_VISIBILITY)) {
			rowId1s = indexWriter.write(
					adapter,
					new AccumuloDataStoreStatsTest.TestGeometry(
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
							"test_pt_1"));

			rowId2s = indexWriter.write(
					adapter,
					new AccumuloDataStoreStatsTest.TestGeometry(
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
							"test_pt_2"));
		}

	}

	@Test
	public void testDeleteByDataId() {
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID);
		assertEquals(
				2,
				countStats.getCount());
		assertTrue(rowId1s.size() > 1);
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				new RowIdQuery(
						rowId1s));
		assertTrue(it1.hasNext());
		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index),
				new DataIdQuery(
						adapter.getAdapterId(),
						new ByteArrayId(
								"test_pt_1"))));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(),
				new RowIdQuery(
						rowId1s));
		assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID);
		assertEquals(
				1,
				countStats.getCount());
	}

	@Test
	public void testDeleteByRowId() {
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID);
		assertEquals(
				2,
				countStats.getCount());
		assertEquals(
				18,
				rowId2s.size());
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				new DataIdQuery(
						adapter.getAdapterId(),
						new ByteArrayId(
								"test_pt_2")));
		assertTrue(it1.hasNext());
		assertTrue(adapter.getDataId(
				(TestGeometry) it1.next()).getString().equals(
				"test_pt_2"));
		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index),
				new RowIdQuery(
						rowId2s)));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(),
				new RowIdQuery(
						rowId2s));
		assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID);
		// TODO: BUG
		assertEquals(
				1,
				countStats.getCount());
	}
}
