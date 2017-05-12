package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.DataStoreFinder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.adapter.vector.export.VectorMRExportCommand;
import mil.nga.giat.geowave.adapter.vector.export.VectorMRExportOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.format.gpx.GpxIngestPlugin;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.mapreduce.dedupe.GeoWaveDedupeJobRunner;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.TestUtils.ExpectedResults;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
public class BasicMapReduceIT
{
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TestUtils.TEST_RESOURCE_PACKAGE
			+ "mapreduce-testdata.zip";
	protected static final String TEST_CASE_GENERAL_GPX_BASE = TestUtils.TEST_CASE_BASE + "general_gpx_test_case/";
	protected static final String GENERAL_GPX_FILTER_PACKAGE = TEST_CASE_GENERAL_GPX_BASE + "filter/";
	protected static final String GENERAL_GPX_FILTER_FILE = GENERAL_GPX_FILTER_PACKAGE + "filter.shp";
	protected static final String GENERAL_GPX_INPUT_GPX_DIR = TEST_CASE_GENERAL_GPX_BASE + "input_gpx/";
	protected static final String GENERAL_GPX_EXPECTED_RESULTS_DIR = TEST_CASE_GENERAL_GPX_BASE + "filter_results/";
	protected static final String OSM_GPX_INPUT_DIR = TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/";

	private static long startMillis;

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		ZipUtils.unZipFile(
				new File(
						MapReduceTestEnvironment.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);

		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING BasicMapReduceIT      *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED BasicMapReduceIT        *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(BasicMapReduceIT.class);
	private static final String TEST_EXPORT_DIRECTORY = "basicMapReduceIT-export";

	public static enum ResultCounterType {
		EXPECTED,
		UNEXPECTED,
		ERROR
	}

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		TestUtils.deleteAll(dataStorePluginOptions);
		MapReduceTestUtils.testMapReduceIngest(
				dataStorePluginOptions,
				DimensionalityType.SPATIAL,
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
		final ExpectedResults expectedResults = TestUtils.getExpectedResults(expectedResultsResources
				.toArray(new URL[expectedResultsResources.size()]));
		runTestJob(
				expectedResults,
				TestUtils.resourceToQuery(new File(
						GENERAL_GPX_FILTER_FILE).toURI().toURL()),
				null,
				null);
	}

	@Test
	public void testIngestOsmGpxMultipleIndices()
			throws Exception {
		TestUtils.deleteAll(dataStorePluginOptions);
		// ingest the data set into multiple indices and then try several query
		// methods, by adapter and by index
		MapReduceTestUtils.testMapReduceIngest(
				dataStorePluginOptions,
				DimensionalityType.ALL,
				OSM_GPX_INPUT_DIR);
		final WritableDataAdapter<SimpleFeature>[] adapters = new GpxIngestPlugin().getDataAdapters(null);

		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = dataStorePluginOptions.createDataStore();
		final Map<ByteArrayId, ExpectedResults> adapterIdToResultsMap = new HashMap<ByteArrayId, ExpectedResults>();
		for (final WritableDataAdapter<SimpleFeature> adapter : adapters) {
			adapterIdToResultsMap.put(
					adapter.getAdapterId(),
					TestUtils.getExpectedResults(geowaveStore.query(
							new QueryOptions(
									adapter),
							new EverythingQuery())));
		}

		final List<DataAdapter<?>> firstTwoAdapters = new ArrayList<DataAdapter<?>>();
		firstTwoAdapters.add(adapters[0]);
		firstTwoAdapters.add(adapters[1]);
		final ExpectedResults firstTwoAdaptersResults = TestUtils.getExpectedResults(geowaveStore.query(
				new QueryOptions(
						firstTwoAdapters),
				new EverythingQuery()));
		final ExpectedResults fullDataSetResults = TestUtils.getExpectedResults(geowaveStore.query(
				new QueryOptions(),
				new EverythingQuery()));
		// just for sanity verify its greater than 0 (ie. that data was actually
		// ingested in the first place)
		Assert.assertTrue(
				"There is no data ingested from OSM GPX test files",
				fullDataSetResults.count > 0);

		// now that we have expected results, run map-reduce export and
		// re-ingest it
		testMapReduceExportAndReingest(DimensionalityType.ALL);
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
				null);

		// now try all adapters and the spatial temporal index, the result
		// should be the full data set
		runTestJob(
				fullDataSetResults,
				null,
				adapters,
				TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX);

		// and finally run with nothing set, should be the full data set
		runTestJob(
				fullDataSetResults,
				null,
				null,
				null);
	}

	private void testMapReduceExportAndReingest(
			final DimensionalityType dimensionalityType )
			throws Exception {
		final VectorMRExportCommand exportCommand = new VectorMRExportCommand();
		final VectorMRExportOptions options = exportCommand.getMrOptions();

		exportCommand.setStoreOptions(dataStorePluginOptions);

		final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();
		final String exportPath = env.getHdfsBaseDirectory() + "/" + TEST_EXPORT_DIRECTORY;

		final File exportDir = new File(
				exportPath.replace(
						"file:",
						""));
		if (exportDir.exists()) {
			boolean deleted = false;
			int attempts = 5;
			while (!deleted && (attempts-- > 0)) {
				try {
					FileUtils.deleteDirectory(exportDir);
					deleted = true;
				}
				catch (final Exception e) {
					LOGGER.error("Export directory not deleted, trying again in 10s: " + e);
					Thread.sleep(10000);
				}
			}
		}

		exportCommand.setParameters(
				env.getHdfs(),
				exportPath,
				null);
		options.setBatchSize(10000);
		options.setMinSplits(MapReduceTestUtils.MIN_INPUT_SPLITS);
		options.setMaxSplits(MapReduceTestUtils.MAX_INPUT_SPLITS);
		options.setResourceManagerHostPort(env.getJobtracker());

		final Configuration conf = MapReduceTestUtils.getConfiguration();
		MapReduceTestUtils.filterConfiguration(conf);
		final int res = ToolRunner.run(
				conf,
				exportCommand.createRunner(new ManualOperationParams()),
				new String[] {});
		Assert.assertTrue(
				"Export Vector Data map reduce job failed",
				res == 0);
		TestUtils.deleteAll(dataStorePluginOptions);
		MapReduceTestUtils.testMapReduceIngest(
				dataStorePluginOptions,
				DimensionalityType.ALL,
				"avro",
				TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + File.separator
						+ TEST_EXPORT_DIRECTORY);
	}

	@SuppressFBWarnings(value = "DM_GC", justification = "Memory usage kept low for travis-ci")
	private void runTestJob(
			final ExpectedResults expectedResults,
			final DistributableQuery query,
			final DataAdapter<?>[] adapters,
			final PrimaryIndex index )
			throws Exception {
		final TestJobRunner jobRunner = new TestJobRunner(
				dataStorePluginOptions,
				expectedResults);
		jobRunner.setMinInputSplits(MapReduceTestUtils.MIN_INPUT_SPLITS);
		jobRunner.setMaxInputSplits(MapReduceTestUtils.MAX_INPUT_SPLITS);
		if (query != null) {
			jobRunner.setQuery(query);
		}
		final QueryOptions options = new QueryOptions();
		if ((adapters != null) && (adapters.length > 0)) {
			options.setAdapters(Arrays.asList(adapters));
		}
		if ((index != null)) {
			options.setIndex(index);
		}
		jobRunner.setQueryOptions(options);
		final Configuration conf = MapReduceTestUtils.getConfiguration();
		MapReduceTestUtils.filterConfiguration(conf);
		final int res = ToolRunner.run(
				conf,
				jobRunner,
				new String[] {});
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
				final DataStorePluginOptions pluginOptions,
				final ExpectedResults expectedResults ) {
			super(
					pluginOptions);
			this.expectedResults = expectedResults;
		}

		@Override
		protected String getHdfsOutputBase() {
			return MapReduceTestEnvironment.getInstance().getHdfsBaseDirectory();
		}

		@Override
		public int runJob()
				throws Exception {
			final boolean job1Success = (super.runJob() == 0);
			Assert.assertTrue(job1Success);
			// after the first job there should be a sequence file with the
			// filtered results which should match the expected results
			// resources

			final Job job = Job.getInstance(super.getConf());

			final Configuration conf = job.getConfiguration();
			MapReduceTestUtils.filterConfiguration(conf);
			final ByteBuffer buf = ByteBuffer.allocate((8 * expectedResults.hashedCentroids.size()) + 4);
			buf.putInt(expectedResults.hashedCentroids.size());
			for (final Long hashedCentroid : expectedResults.hashedCentroids) {
				buf.putLong(hashedCentroid);
			}
			conf.set(
					MapReduceTestUtils.EXPECTED_RESULTS_KEY,
					ByteArrayUtils.byteArrayToString(buf.array()));

			GeoWaveInputFormat.setStoreOptions(
					conf,
					dataStoreOptions);
			job.setJarByClass(this.getClass());

			job.setJobName("GeoWave Test (" + dataStoreOptions.getGeowaveNamespace() + ")");
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapperClass(VerifyExpectedResultsMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);
			job.setSpeculativeExecution(false);
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
					resultType = expectedHashedCentroids.contains(TestUtils.hashCentroid(geometry)) ? ResultCounterType.EXPECTED
							: ResultCounterType.UNEXPECTED;
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
			final String expectedResults = config.get(MapReduceTestUtils.EXPECTED_RESULTS_KEY);
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
