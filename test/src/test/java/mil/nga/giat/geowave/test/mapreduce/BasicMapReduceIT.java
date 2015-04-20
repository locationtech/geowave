package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.dedupe.GeoWaveDedupeJobRunner;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.format.gpx.GpxIngestPlugin;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.geotools.data.DataStoreFinder;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class BasicMapReduceIT extends
		MapReduceTestBase
{
	private final static Logger LOGGER = Logger.getLogger(BasicMapReduceIT.class);

	public static enum ResultCounterType {
		EXPECTED,
		UNEXPECTED,
		ERROR
	}

	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error(
					"Unable to clear accumulo namespace",
					ex);
			Assert.fail("Index not deleted successfully");
		}
		testMapReduceIngest(
				IndexType.SPATIAL_VECTOR,
				GENERAL_GPX_INPUT_GPX_DIR);
		final File gpxInputDir = new File(
				GENERAL_GPX_INPUT_GPX_DIR);
		final File expectedResultsDir = new File(
				GENERAL_GPX_EXPECTED_RESULTS_DIR);
		final List<URL> expectedResultsResources = new ArrayList<URL>();
		final Map<String, URL> baseNameToExpectedResultURL = new HashMap<String, URL>();

		for (final File file : expectedResultsDir.listFiles(new FileFilter() {

			@Override
			public boolean accept(
					final File pathname ) {
				final Map<String, Object> map = new HashMap<String, Object>();
				try {
					map.put(
							"url",
							pathname.toURI().toURL());
					return DataStoreFinder.getDataStore(map) != null;
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Cannot read file as GeoTools data store",
							e);
				}
				return false;
			}

		})) {
			baseNameToExpectedResultURL.put(
					FilenameUtils.getBaseName(
							file.getName()).replaceAll(
							"_filtered",
							""),
					file.toURI().toURL());
		}
		for (final String filename : gpxInputDir.list(new FilenameFilter() {
			@Override
			public boolean accept(
					final File dir,
					final String name ) {
				return FilenameUtils.isExtension(
						name,
						new GpxIngestPlugin().getFileExtensionFilters());
			}
		})) {
			final URL url = baseNameToExpectedResultURL.get(FilenameUtils.getBaseName(filename));
			Assert.assertNotNull(url);
			expectedResultsResources.add(url);
		}
		final ExpectedResults expectedResults = getExpectedResults(expectedResultsResources.toArray(new URL[expectedResultsResources.size()]));
		runTestJob(
				expectedResults,
				resourceToQuery(new File(
						GENERAL_GPX_FILTER_FILE).toURI().toURL()),
				null,
				null);
	}

	@Test
	public void testIngestOsmGpxMultipleIndices()
			throws Exception {
		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error(
					"Unable to clear accumulo namespace",
					ex);
			Assert.fail("Index not deleted successfully");
		}
		// ingest the data set into multiple indices and then try several query
		// methods, by adapter and by index
		testMapReduceIngest(
				IndexType.SPATIAL_VECTOR,
				OSM_GPX_INPUT_DIR);
		testMapReduceIngest(
				IndexType.SPATIAL_TEMPORAL_VECTOR,
				OSM_GPX_INPUT_DIR);
		final WritableDataAdapter<SimpleFeature>[] adapters = new GpxIngestPlugin().getDataAdapters(null);

		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);
		final Map<ByteArrayId, ExpectedResults> adapterIdToResultsMap = new HashMap<ByteArrayId, GeoWaveTestEnvironment.ExpectedResults>();
		for (final WritableDataAdapter<SimpleFeature> adapter : adapters) {
			adapterIdToResultsMap.put(
					adapter.getAdapterId(),
					getExpectedResults(geowaveStore.query(
							adapter,
							null)));
		}

		final List<ByteArrayId> firstTwoAdapters = new ArrayList<ByteArrayId>();
		firstTwoAdapters.add(adapters[0].getAdapterId());
		firstTwoAdapters.add(adapters[1].getAdapterId());
		final ExpectedResults firstTwoAdaptersResults = getExpectedResults(geowaveStore.query(
				firstTwoAdapters,
				null));
		final ExpectedResults fullDataSetResults = getExpectedResults(geowaveStore.query(null));
		// just for sanity verify its greater than 0 (ie. that data was actually
		// ingested in the first place)
		Assert.assertTrue(
				"There is no data ingested from OSM GPX test files",
				fullDataSetResults.count > 0);
		// first try each adapter individually
		for (final WritableDataAdapter<SimpleFeature> adapter : adapters) {
			runTestJob(
					adapterIdToResultsMap.get(adapter.getAdapterId()),
					null,
					new DataAdapter[] {
						adapter
					},
					null);
		}
		// then try the first 2 adapters, and may as well try with both indices
		// set (should be the default behavior anyways)
		runTestJob(
				firstTwoAdaptersResults,
				null,
				new DataAdapter[] {
					adapters[0],
					adapters[1]
				},
				new Index[] {
					IndexType.SPATIAL_VECTOR.createDefaultIndex(),
					IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex()
				});

		// now try all adapters and the spatial temporal index, the result
		// should be the full data set
		runTestJob(
				fullDataSetResults,
				null,
				adapters,
				new Index[] {
					IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex()
				});

		// and finally run with nothing set, should be the full data set
		runTestJob(
				fullDataSetResults,
				null,
				null,
				null);
	}

	@SuppressFBWarnings(value = "DM_GC", justification = "Memory usage kept low for travis-ci")
	private void runTestJob(
			final ExpectedResults expectedResults,
			final DistributableQuery query,
			final DataAdapter<?>[] adapters,
			final Index[] indices )
			throws Exception {
		final TestJobRunner jobRunner = new TestJobRunner(
				expectedResults);
		jobRunner.setMinInputSplits(MIN_INPUT_SPLITS);
		jobRunner.setMaxInputSplits(MAX_INPUT_SPLITS);
		if (query != null) {
			jobRunner.setQuery(query);
		}
		if ((adapters != null) && (adapters.length > 0)) {
			for (final DataAdapter<?> adapter : adapters) {
				jobRunner.addDataAdapter(adapter);
			}
		}
		if ((indices != null) && (indices.length > 0)) {
			for (final Index index : indices) {
				jobRunner.addIndex(index);
			}
		}
		final Configuration conf = getConfiguration();
		MapReduceTestEnvironment.filterConfiguration(conf);
		final int res = ToolRunner.run(
				conf,
				jobRunner,
				new String[] {
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					TEST_NAMESPACE
				});
		Assert.assertEquals(
				0,
				res);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private static class TestJobRunner extends
			GeoWaveDedupeJobRunner
	{
		private final ExpectedResults expectedResults;

		public TestJobRunner(
				final ExpectedResults expectedResults ) {
			this.expectedResults = expectedResults;
		}

		@Override
		protected String getHdfsOutputBase() {
			return hdfsBaseDirectory;
		}

		@Override
		public int runJob()
				throws Exception {
			final boolean job1Success = (super.runJob() == 0);
			Assert.assertTrue(job1Success);
			// after the first job there should be a sequence file with the
			// filtered results which should match the expected results
			// resources
			final Configuration conf = super.getConf();
			MapReduceTestEnvironment.filterConfiguration(conf);
			final ByteBuffer buf = ByteBuffer.allocate((8 * expectedResults.hashedCentroids.size()) + 4);
			buf.putInt(expectedResults.hashedCentroids.size());
			for (final Long hashedCentroid : expectedResults.hashedCentroids) {
				buf.putLong(hashedCentroid);
			}
			conf.set(
					EXPECTED_RESULTS_KEY,
					ByteArrayUtils.byteArrayToString(buf.array()));
			final Job job = Job.getInstance(conf);
			job.setJarByClass(this.getClass());

			job.setJobName("GeoWave Test (" + namespace + ")");
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapperClass(VerifyExpectedResultsMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);
			job.setSpeculativeExecution(false);

			GeoWaveInputFormat.setAccumuloOperationsInfo(
					job.getConfiguration(),
					zookeeper,
					instance,
					user,
					password,
					namespace);
			FileInputFormat.setInputPaths(
					job,
					getHdfsOutputPath());

			final boolean job2success = job.waitForCompletion(true);
			final Counters jobCounters = job.getCounters();
			final Counter expectedCnt = jobCounters.findCounter(ResultCounterType.EXPECTED);
			Assert.assertNotNull(expectedCnt);
			Assert.assertEquals(
					expectedResults.count,
					expectedCnt.getValue());
			final Counter errorCnt = jobCounters.findCounter(ResultCounterType.ERROR);
			if (errorCnt != null) {
				Assert.assertEquals(
						0L,
						errorCnt.getValue());
			}
			final Counter unexpectedCnt = jobCounters.findCounter(ResultCounterType.UNEXPECTED);
			if (unexpectedCnt != null) {
				Assert.assertEquals(
						0L,
						unexpectedCnt.getValue());
			}
			return job2success ? 0 : 1;
		}
	}

	private static class VerifyExpectedResultsMapper extends
			GeoWaveWritableInputMapper<NullWritable, NullWritable>
	{
		private Set<Long> expectedHashedCentroids = new HashSet<Long>();

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final Mapper<GeoWaveInputKey, ObjectWritable, NullWritable, NullWritable>.Context context )
				throws IOException,
				InterruptedException {
			ResultCounterType resultType = ResultCounterType.ERROR;
			if (value instanceof SimpleFeature) {
				final SimpleFeature result = (SimpleFeature) value;
				final Geometry geometry = (Geometry) result.getDefaultGeometry();
				if (!geometry.isEmpty()) {
					resultType = expectedHashedCentroids.contains(hashCentroid(geometry)) ? ResultCounterType.EXPECTED : ResultCounterType.UNEXPECTED;
				}
			}
			context.getCounter(
					resultType).increment(
					1);
		}

		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, NullWritable, NullWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final Configuration config = GeoWaveConfiguratorBase.getConfiguration(context);
			final String expectedResults = config.get(EXPECTED_RESULTS_KEY);
			if (expectedResults != null) {
				expectedHashedCentroids = new HashSet<Long>();
				final byte[] expectedResultsBinary = ByteArrayUtils.byteArrayFromString(expectedResults);
				final ByteBuffer buf = ByteBuffer.wrap(expectedResultsBinary);
				final int count = buf.getInt();
				for (int i = 0; i < count; i++) {
					expectedHashedCentroids.add(buf.getLong());
				}
			}
		}
	}
}
