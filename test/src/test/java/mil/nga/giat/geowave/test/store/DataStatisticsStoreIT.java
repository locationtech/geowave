package mil.nga.giat.geowave.test.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.bigtable.operations.BigTableOperations;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.CombineStatisticsCommand;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveIT;

/**
 * This special class is here to test the HBase implementation of merging
 * statistics.. Basically it's made more complicated by the fact that HBase
 * doesn't have a MergingCombiner, so we do this special hack here in
 * DataStatistics to merge them as we pull the data out of the iterator.
 */
@RunWith(GeoWaveITRunner.class)
public class DataStatisticsStoreIT extends
		AbstractGeoWaveIT
{
	@GeoWaveTestStore({
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}

	private final static Logger LOGGER = Logger.getLogger(DataStatisticsStoreIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*    RUNNING DataStatisticsStoreIT      *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*    FINISHED DataStatisticsStoreIT     *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testInsert()
			throws IOException {
		final DataStatisticsStore store = dataStore.createDataStatisticsStore();

		final CountDataStatistics<String> stat = new CountDataStatistics<String>(
				new ByteArrayId(
						"blah"));
		stat.entryIngested(
				null,
				null);
		stat.entryIngested(
				null,
				null);
		stat.entryIngested(
				null,
				null);

		store.incorporateStatistics(stat);

		final BasicHBaseOperations ops = createOperations();

		final Scan scan = new Scan();
		scan.setStartRow(stat.getStatisticsId().getBytes());
		scan.setStopRow(stat.getStatisticsId().getNextPrefix());
		scan.addFamily(new ByteArrayId(
				"STATS").getBytes());

		final ResultScanner rs = ops.getScannedResults(
				scan,
				"GEOWAVE_METADATA");
		final Iterator<Result> res = rs.iterator();
		final byte[] row = res.next().getRow();
		Assert.assertEquals(
				stat.getStatisticsId().getBytes().length + DataStoreUtils.UNIQUE_ADDED_BYTES,
				row.length);

		final byte[] bytes = new byte[stat.getStatisticsId().getBytes().length];
		final ByteBuffer bb = ByteBuffer.wrap(row);
		bb.get(bytes);
		final ByteArrayId bad = new ByteArrayId(
				bytes);
		Assert.assertEquals(
				stat.getStatisticsId().getString(),
				bad.getString());
		final CountDataStatistics<String> storedStat = (CountDataStatistics<String>) store.getDataStatistics(
				stat.getDataAdapterId(),
				stat.getStatisticsId());
		Assert.assertEquals(
				stat.getCount(),
				storedStat.getCount());
		Assert.assertEquals(
				stat.getDataAdapterId(),
				storedStat.getDataAdapterId());
		Assert.assertEquals(
				stat.getStatisticsId(),
				storedStat.getStatisticsId());
		Assert.assertArrayEquals(
				stat.getVisibility(),
				storedStat.getVisibility());
	}

	private BasicHBaseOperations createOperations()
			throws IOException {
		if (dataStore.getFactoryOptions() instanceof HBaseRequiredOptions) {
			final HBaseRequiredOptions opts = (HBaseRequiredOptions) dataStore.getFactoryOptions();
			return BasicHBaseOperations.createOperations(opts);
		}
		else if (dataStore.getFactoryOptions() instanceof BigTableOptions) {
			final BigTableOptions opts = (BigTableOptions) dataStore.getFactoryOptions();
			return BigTableOperations.createOperations(opts);
		}
		return null;
	}

	@Test
	public void testQuery()
			throws IOException {
		ByteArrayId adapterId = null;
		ByteArrayId statisticsId = null;
		for (int i = 0; i < 3; i++) {
			final DataStatisticsStore store = dataStore.createDataStatisticsStore();
			final DataStatistics<?> stat = new CountDataStatistics<String>(
					new ByteArrayId(
							"blah2"));
			stat.entryIngested(
					null,
					null);
			stat.entryIngested(
					null,
					null);
			stat.entryIngested(
					null,
					null);
			statisticsId = stat.getStatisticsId();
			adapterId = stat.getDataAdapterId();
			store.incorporateStatistics(stat);
		}

		final DataStatisticsStore store = dataStore.createDataStatisticsStore();

		@SuppressWarnings("unchecked")
		final CountDataStatistics<String> stat = (CountDataStatistics<String>) store.getDataStatistics(
				adapterId,
				statisticsId);

		// Non-cached version
		Assert.assertEquals(
				9,
				stat.getCount());

		@SuppressWarnings("unchecked")
		final CountDataStatistics<String> stat2 = (CountDataStatistics<String>) store.getDataStatistics(
				adapterId,
				statisticsId);

		// Cached version
		Assert.assertEquals(
				9,
				stat2.getCount());

		// This code then makes sure that there are indeed 3 records in the
		// database!
		final BasicHBaseOperations ops = createOperations();

		final Scan scan = new Scan();
		scan.setStartRow(statisticsId.getBytes());
		scan.setStopRow(statisticsId.getNextPrefix());
		scan.addFamily(new ByteArrayId(
				"STATS").getBytes());

		final ResultScanner rs = ops.getScannedResults(
				scan,
				"GEOWAVE_METADATA");

		int count = 0;
		final Iterator<Result> res = rs.iterator();
		while (res.hasNext()) {
			res.next();
			count += 1;
		}

		Assert.assertEquals(
				3,
				count);
	}

	@Test
	public void testDelete()
			throws IOException {
		ByteArrayId adapterId = null;
		ByteArrayId statisticsId = null;
		for (int i = 0; i < 3; i++) {
			final DataStatisticsStore store = dataStore.createDataStatisticsStore();
			final DataStatistics<?> stat = new CountDataStatistics<String>(
					new ByteArrayId(
							"blah3"));
			stat.entryIngested(
					null,
					null);
			stat.entryIngested(
					null,
					null);
			stat.entryIngested(
					null,
					null);
			statisticsId = stat.getStatisticsId();
			adapterId = stat.getDataAdapterId();
			store.incorporateStatistics(stat);
		}

		final DataStatisticsStore store = dataStore.createDataStatisticsStore();

		// Must load it up first... (so it gets into cache)
		final Object ignoreVal = store.getDataStatistics(
				adapterId,
				statisticsId);
		Assert.assertNotNull(ignoreVal);

		store.removeStatistics(
				adapterId,
				statisticsId);

		final Object val = store.getDataStatistics(
				adapterId,
				statisticsId);
		Assert.assertNull(val);

		// This code then makes sure that there are indeed 0 records in the
		// database!
		final BasicHBaseOperations ops = createOperations();

		final Scan scan = new Scan();
		scan.setStartRow(statisticsId.getBytes());
		scan.setStopRow(statisticsId.getNextPrefix());
		scan.addColumn(
				new ByteArrayId(
						"STATS").getBytes(),
				new ByteArrayId(
						"blah3").getBytes());

		final ResultScanner rs = ops.getScannedResults(
				scan,
				"GEOWAVE_METADATA");

		int count = 0;
		final Iterator<Result> res = rs.iterator();
		while (res.hasNext()) {
			res.next();
			count += 1;
		}

		Assert.assertEquals(
				0,
				count);
	}

	@Test
	public void testCombineCommand()
			throws IOException {

		// Insert Records
		ByteArrayId statisticsId = null;
		for (int i = 0; i < 3; i++) {
			final DataStatisticsStore store = dataStore.createDataStatisticsStore();
			final DataStatistics<?> stat = new CountDataStatistics<String>(
					new ByteArrayId(
							"blah4"));
			stat.entryIngested(
					null,
					null);
			stat.entryIngested(
					null,
					null);
			stat.entryIngested(
					null,
					null);
			statisticsId = stat.getStatisticsId();
			store.incorporateStatistics(stat);
		}

		// Combine the statistics
		final CombineStatisticsCommand combineCommand = new CombineStatisticsCommand();
		combineCommand.setInputStoreOptions(dataStore);
		combineCommand.setParameters(
				null,
				"blah4");
		combineCommand.execute(new ManualOperationParams());

		// Assert that there is only one row
		final BasicHBaseOperations ops = createOperations();

		final Scan scan = new Scan();
		scan.setStartRow(statisticsId.getBytes());
		scan.setStopRow(statisticsId.getNextPrefix());
		scan.addColumn(
				new ByteArrayId(
						"STATS").getBytes(),
				new ByteArrayId(
						"blah4").getBytes());

		final ResultScanner rs = ops.getScannedResults(
				scan,
				"GEOWAVE_METADATA");

		int count = 0;
		final Iterator<Result> res = rs.iterator();
		Result r = null;
		while (res.hasNext()) {
			r = res.next();
			count += 1;
		}

		Assert.assertEquals(
				1,
				count);

		// Assert it has the right value.
		final Cell cell = r.listCells().get(
				0);
		@SuppressWarnings("unchecked")
		final CountDataStatistics<String> stat = (CountDataStatistics<String>) PersistenceUtils.fromBinary(
				CellUtil.cloneValue(cell),
				Persistable.class);

		Assert.assertEquals(
				9,
				stat.getCount());
	}
}
