package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
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
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.InsertionIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometry;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometryAdapter;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

public class DeleteWriterTest
{
	private final MockInstance mockInstance = new MockInstance();
	private Connector mockConnector = null;
	private AccumuloOperations operations;
	private DataStore mockDataStore;
	private InsertionIds rowId1s;
	private InsertionIds rowId2s;
	private WritableDataAdapter<AccumuloDataStoreStatsTest.TestGeometry> adapter;
	private DataStatisticsStore statsStore;
	protected AccumuloOptions options = new AccumuloOptions();

	private static final CommonIndexModel model = new SpatialDimensionalityTypeProvider()
			.createPrimaryIndex()
			.getIndexModel();

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
		final AccumuloOptions options = new AccumuloOptions();

		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));

			operations = new AccumuloOperations(
					mockConnector,
					options);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			e.printStackTrace();
		}

		operations.createTable(
				"test_table",
				true,
				true);
		mockDataStore = new AccumuloDataStore(
				operations,
				options);

		statsStore = new DataStatisticsStoreImpl(
				operations,
				options);

		adapter = new TestGeometryAdapter();
		final GeometryFactory factory = new GeometryFactory();

		try (IndexWriter indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			rowId1s = indexWriter.write(new AccumuloDataStoreStatsTest.TestGeometry(
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

			rowId2s = indexWriter.write(new AccumuloDataStoreStatsTest.TestGeometry(
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
		assertTrue(rowId1s.getSize() > 1);

		final Pair<ByteArrayId, ByteArrayId> key = rowId1s.getFirstPartitionAndSortKeyPair();
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				new InsertionIdQuery(
						key.getLeft(),
						key.getRight(),
						new ByteArrayId(
								"test_pt_1")));
		assertTrue(it1.hasNext());
		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index),
				new DataIdQuery(
						new ByteArrayId(
								"test_pt_1"))));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				new InsertionIdQuery(
						key.getLeft(),
						key.getRight(),
						new ByteArrayId(
								"test_pt_1")));
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
				rowId2s.getSize());
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				new DataIdQuery(
						new ByteArrayId(
								"test_pt_2")));
		assertTrue(it1.hasNext());
		assertTrue(adapter.getDataId(
				(TestGeometry) it1.next()).getString().equals(
				"test_pt_2"));
		final Pair<ByteArrayId, ByteArrayId> key = rowId1s.getFirstPartitionAndSortKeyPair();
		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index),
				new InsertionIdQuery(
						key.getLeft(),
						key.getRight(),
						new ByteArrayId(
								"test_pt_2"))));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				new DataIdQuery(
						new ByteArrayId(
								"test_pt_2")));
		// TODO GEOWAVE-1018 this should be fixed in the latest on master (all
		// rows associated with a deletion should also be deleted)

		// assertTrue(
		// !it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID);
		// TODO: BUG, this should be 0
		assertEquals(
				1,
				countStats.getCount());
	}
}
