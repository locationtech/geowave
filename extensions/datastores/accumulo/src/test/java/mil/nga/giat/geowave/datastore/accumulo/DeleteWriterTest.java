package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
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
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometry;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometryAdapter;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;

public class DeleteWriterTest
{
	private final MockInstance mockInstance = new MockInstance();
	private Connector mockConnector = null;
	private BasicAccumuloOperations operations;
	private DataStore mockDataStore;
	private List<ByteArrayId> rowId1s;
	private List<ByteArrayId> rowId2s;
	private List<ByteArrayId> rowId3s;
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

		operations.createTable(
				"test_table",
				true,
				true,
				null);
		mockDataStore = new AccumuloDataStore(
				operations,
				options);

		statsStore = new AccumuloDataStatisticsStore(
				operations);

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
					"test_line_1"));

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
					"test_line_2"));
			rowId3s = indexWriter.write(new AccumuloDataStoreStatsTest.TestGeometry(
					factory.createPoint(new Coordinate(
							-77.0352,
							38.8895)),
					"test_pt_1"));
		}

	}

	@Test
	public void testDeleteByDataId() {
		BaseDataStore.testBulkDelete = false;
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
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
								"test_line_1"))));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(),
				new RowIdQuery(
						rowId1s));
		assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}

	@Test
	public void testDeleteByRowId() {
		BaseDataStore.testBulkDelete = false;
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		assertEquals(
				18,
				rowId2s.size());
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				new DataIdQuery(
						adapter.getAdapterId(),
						new ByteArrayId(
								"test_line_2")));
		assertTrue(it1.hasNext());
		assertTrue(adapter.getDataId(
				(TestGeometry) it1.next()).getString().equals(
				"test_line_2"));
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
				CountDataStatistics.STATS_TYPE);
		// TODO: BUG
		assertEquals(
				2,
				countStats.getCount());
	}

	@Test
	public void testDeleteBySpatialConstraint() {
		BaseDataStore.testBulkDelete = true;
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		SpatialQuery spatialQuery = new SpatialQuery(
				new GeometryFactory().toGeometry(new Envelope(
						-78,
						-77,
						38,
						39)));
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				spatialQuery);
		assertTrue(it1.hasNext());
		assertTrue(adapter.getDataId(
				(TestGeometry) it1.next()).getString().equals(
				"test_pt_1"));
		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index),
				spatialQuery));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(),
				spatialQuery);
		assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}
	
	@Test
	public void testDeleteByRowIds() {
		BaseDataStore.testBulkDelete = true;
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		assertEquals(
				18,
				rowId2s.size());
		RowIdQuery rowIdQuery = new RowIdQuery(rowId2s);
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				rowIdQuery);
		assertTrue(it1.hasNext());
		assertTrue(adapter.getDataId(
				(TestGeometry) it1.next()).getString().equals(
				"test_line_2"));
		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index),
				rowIdQuery));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(),
				rowIdQuery);
		assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}
	
	@Test
	 public void testDeleteByPrefixId() {
		BaseDataStore.testBulkDelete = true;
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		assertEquals(
				18,
				rowId2s.size());
		PrefixIdQuery prefixIdQuery = new PrefixIdQuery(new ByteArrayId(
				"test_line_2"));
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				prefixIdQuery);
		assertTrue(it1.hasNext());
		assertTrue(adapter.getDataId(
				(TestGeometry) it1.next()).getString().equals(
				"test_line_1"));
		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index),
				prefixIdQuery));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(),
				prefixIdQuery);
		//assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}
}
