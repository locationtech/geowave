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
package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
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
import org.apache.commons.lang3.tuple.Pair;
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
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.InsertionIdQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometry;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometryAdapter;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

public class DeleteWriterTest
{
	private static final Logger LOGGER = LoggerFactory.getLogger(DeleteWriterTest.class);
	private AccumuloOperations operations;
	private DataStore mockDataStore;
	private InsertionIds rowIds1;
	private InsertionIds rowIds2;
	private InsertionIds rowIds3;
	private WritableDataAdapter<AccumuloDataStoreStatsTest.TestGeometry> adapter;
	private DataStatisticsStore statsStore;
	InternalAdapterStore internalAdapterStore;
	protected AccumuloOptions options = new AccumuloOptions();

	private static final CommonIndexModel MODEL = new SpatialDimensionalityTypeProvider().createPrimaryIndex(
			new SpatialOptions()).getIndexModel();

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
			operations = new AccumuloOperations(
					mockConnector,
					options);
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
			operations = new AccumuloOperations(
					miniAccumulo.getConnector(
							"root",
							new PasswordToken(
									DEFAULT_MINI_ACCUMULO_PASSWORD)),
					new AccumuloOptions());
		}
		operations.createTable(
				"test_table",
				true,
				true);
		mockDataStore = new AccumuloDataStore(
				operations,
				options);

		internalAdapterStore = new InternalAdapterStoreImpl(
				operations);

		statsStore = new DataStatisticsStoreImpl(
				operations,
				options);

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
	public void testDeleteByInsertionId() {
		short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		assertTrue(rowIds1.getSize() > 1);

		final Pair<ByteArrayId, ByteArrayId> key = rowIds1.getFirstPartitionAndSortKeyPair();
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				new InsertionIdQuery(
						key.getLeft(),
						key.getRight(),
						new ByteArrayId(
								"test_line_1")));
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
				internalAdapterId,
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}

	@Test
	public void testDeleteBySpatialConstraint() {
		short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		final SpatialQuery spatialQuery = new SpatialQuery(
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
				internalAdapterId,
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}

	@Test
	public void testDeleteByPrefixId() {
		short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		final ByteArrayId rowId3 = rowIds3.getCompositeInsertionIds().get(
				0);
		// just take the first half of the row ID as the prefix
		final byte[] rowId3Prefix = Arrays.copyOf(
				rowId3.getBytes(),
				rowId3.getBytes().length / 2);

		final PrefixIdQuery prefixIdQuery = new PrefixIdQuery(
				null,
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
				internalAdapterId,
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				2,
				countStats.getCount());
	}

	@Test
	public void testDeleteByDataId() {
		short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE);
		assertEquals(
				3,
				countStats.getCount());
		assertTrue(rowIds1.getSize() > 1);
		final Pair<ByteArrayId, ByteArrayId> key = rowIds1.getFirstPartitionAndSortKeyPair();
		final CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index),
				new InsertionIdQuery(
						key.getLeft(),
						key.getRight(),
						new ByteArrayId(
								"test_line_1")));
		assertTrue(it1.hasNext());
		assertTrue(((BaseDataStore) mockDataStore).delete(
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
				new DataIdQuery(
						new ByteArrayId(
								"test_pt_1")));
		// TODO GEOWAVE-1018 this should be fixed in the latest on master (all
		// rows associated with a deletion should also be deleted)

		// assertTrue(
		// !it2.hasNext());
		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE);
		// TODO: BUG, this should be 0
		assertEquals(
				2,
				countStats.getCount());
	}
}
