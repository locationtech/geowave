package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;

public class DeleteWriterTest
{
	private static final Logger LOGGER = LoggerFactory.getLogger(DeleteWriterTest.class);
	private BasicAccumuloOperations operations;
	private DataStore mockDataStore;
	private List<ByteArrayId> rowIds1;
	private List<ByteArrayId> rowIds2;
	private List<ByteArrayId> rowIds3;
	private WritableDataAdapter<AccumuloDataStoreStatsTest.TestGeometry> adapter;
	private DataStatisticsStore statsStore;
	protected AccumuloOptions options = new AccumuloOptions();

	private static final CommonIndexModel MODEL = new SpatialDimensionalityTypeProvider()
			.createPrimaryIndex()
			.getIndexModel();

	private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition()
	};

	private static final NumericIndexStrategy STRATEGY = TieredSFCIndexFactory.createSingleTierStrategy(
			SPATIAL_DIMENSIONS,
			new int[] {
				16,
				16
			},
			SFCType.HILBERT);

	final SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.S");

	private static final PrimaryIndex index = new PrimaryIndex(
			STRATEGY,
			MODEL);

	protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";
	protected static final String HADOOP_DLL = "hadoop.dll";
	// breaks on windows if temp directory isn't on same drive as project
	protected static final File TEMP_DIR = new File(
			"./target/accumulo_temp");
	private static final boolean USE_MOCK = true;
	protected MiniAccumuloClusterImpl miniAccumulo;
	protected String zookeeper;
	// just increment port so there is no potential conflict
	protected static int port = 2181;

	@Before
	public void setUp()
			throws IOException,
			InterruptedException,
			AccumuloException,
			AccumuloSecurityException {
		if (USE_MOCK) {
			Connector mockConnector;
			mockConnector = new MockInstance().getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
			operations = new BasicAccumuloOperations(
					mockConnector);
		}
		else {
			if (TEMP_DIR.exists()) {
				FileUtils.deleteDirectory(TEMP_DIR);
			}
			zookeeper = "localhost:" + port;
			final MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(
					TEMP_DIR,
					DEFAULT_MINI_ACCUMULO_PASSWORD);
			config.setZooKeeperPort(port++);
			config.setNumTservers(2);

			miniAccumulo = MiniAccumuloClusterFactory.newAccumuloCluster(
					config,
					DeleteWriterTest.class);

			startMiniAccumulo(config);
			operations = new BasicAccumuloOperations(
					miniAccumulo.getConnector(
							"root",
							new PasswordToken(
									DEFAULT_MINI_ACCUMULO_PASSWORD)));
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
			rowIds1 = indexWriter.write(new AccumuloDataStoreStatsTest.TestGeometry(
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

			rowIds2 = indexWriter.write(new AccumuloDataStoreStatsTest.TestGeometry(
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
			rowIds3 = indexWriter.write(new AccumuloDataStoreStatsTest.TestGeometry(
					factory.createPoint(new Coordinate(
							-77.0352,
							38.8895)),
					"test_pt_1"));
		}

	}

	@After
	public void tearDown() {
		if (!USE_MOCK) {
			try {
				miniAccumulo.stop();
			}
			catch (final InterruptedException | IOException e) {
				LOGGER.warn(
						"unable to stop mini accumulo",
						e);
			}
			if (TEMP_DIR != null) {
				try {
					// sleep because mini accumulo processes still have a
					// hold on the log files and there is no hook to get
					// notified when it is completely stopped

					Thread.sleep(2000);
					FileUtils.deleteDirectory(TEMP_DIR);
				}
				catch (final IOException | InterruptedException e) {
					LOGGER.warn(
							"unable to delete temp directory",
							e);
				}
			}
		}
	}

	private void startMiniAccumulo(
			final MiniAccumuloConfigImpl config )
			throws IOException,
			InterruptedException {

		final LinkedList<String> jvmArgs = new LinkedList<>();
		jvmArgs.add("-XX:CompressedClassSpaceSize=512m");
		jvmArgs.add("-XX:MaxMetaspaceSize=512m");
		jvmArgs.add("-Xmx512m");

		Runtime.getRuntime().addShutdownHook(
				new Thread() {
					@Override
					public void run() {
						tearDown();
					}
				});
		final Map<String, String> siteConfig = config.getSiteConfig();
		siteConfig.put(
				Property.INSTANCE_ZK_HOST.getKey(),
				zookeeper);
		config.setSiteConfig(siteConfig);
		miniAccumulo.start();
	}

	@Test
	public void testDeleteByRowId() {
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		assertEquals(
				18,
				rowIds2.size());
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				new RowIdQuery(
						rowIds2));
		assertTrue(it1.hasNext());
		assertTrue(adapter.getDataId(
				(TestGeometry) it1.next()).getString().equals(
				"test_line_2"));
		assertTrue(((BaseDataStore) mockDataStore).delete(
				new QueryOptions(),
				new RowIdQuery(
						rowIds2)));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(),
				new RowIdQuery(
						rowIds2));
		assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}

	@Test
	public void testDeleteBySpatialConstraint() {
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
				new QueryOptions(),
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
	public void testDeleteByPrefixId() {
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		assertEquals(
				1,
				rowIds3.size());
		ByteArrayId rowId3 = rowIds3.get(0);
		// just take the first half of the row ID as the prefix
		byte[] rowId3Prefix = Arrays.copyOf(
				rowId3.getBytes(),
				rowId3.getBytes().length / 2);

		PrefixIdQuery prefixIdQuery = new PrefixIdQuery(
				new ByteArrayId(
						rowId3Prefix));
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				prefixIdQuery);
		assertTrue(it1.hasNext());
		assertTrue(adapter.getDataId(
				(TestGeometry) it1.next()).getString().equals(
				"test_pt_1"));
		assertTrue(mockDataStore.delete(
				new QueryOptions(),
				prefixIdQuery));
		final CloseableIterator it2 = mockDataStore.query(
				new QueryOptions(),
				prefixIdQuery);
		assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}

	@Test
	public void testDeleteByDataId() {
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		assertTrue(rowIds1.size() > 1);
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(),
				new RowIdQuery(
						rowIds1));
		assertTrue(it1.hasNext());
		assertTrue(((BaseDataStore) mockDataStore).delete(
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
						rowIds1));
		assertTrue(!it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}
}
