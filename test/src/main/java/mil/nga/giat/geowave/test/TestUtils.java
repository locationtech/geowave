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
package mil.nga.giat.geowave.test;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.ConfigAWSCommand;
import mil.nga.giat.geowave.core.ingest.operations.LocalToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.ingest.spark.SparkCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.spark.SparkIngestDriver;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.ListStatsCommand;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeRegistry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.beust.jcommander.ParameterException;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalOptions;

public class TestUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

	public static enum DimensionalityType {
		SPATIAL(
				"spatial"),
		SPATIAL_TEMPORAL(
				"spatial_temporal"),
		ALL(
				"spatial,spatial_temporal");
		private final String dimensionalityArg;

		private DimensionalityType(
				final String dimensionalityArg ) {
			this.dimensionalityArg = dimensionalityArg;
		}

		public String getDimensionalityArg() {
			return dimensionalityArg;
		}
	}

	public static final File TEMP_DIR = new File(
			"./target/temp");

	public static final String TEST_FILTER_START_TIME_ATTRIBUTE_NAME = "StartTime";
	public static final String TEST_FILTER_END_TIME_ATTRIBUTE_NAME = "EndTime";
	public static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test";
	public static final String TEST_RESOURCE_PACKAGE = "mil/nga/giat/geowave/test/";
	public static final String TEST_CASE_BASE = "data/";

	public static final PrimaryIndex DEFAULT_SPATIAL_INDEX = new SpatialDimensionalityTypeProvider()
			.createPrimaryIndex(new SpatialOptions());
	public static final PrimaryIndex DEFAULT_SPATIAL_TEMPORAL_INDEX = new SpatialTemporalDimensionalityTypeProvider()
			.createPrimaryIndex(new SpatialTemporalOptions());
	// CRS for Web Mercator
	public static String CUSTOM_CRSCODE = "EPSG:3857";

	public static final CoordinateReferenceSystem CUSTOM_CRS;

	static {
		try {
			CUSTOM_CRS = CRS.decode(
					CUSTOM_CRSCODE,
					true);
		}
		catch (final FactoryException e) {
			LOGGER.error(
					"Unable to decode " + CUSTOM_CRSCODE + "CRS",
					e);
			throw new RuntimeException(
					"Unable to initialize " + CUSTOM_CRSCODE + " CRS");
		}
	}

	public static PrimaryIndex createCustomCRSPrimaryIndex() {
		SpatialDimensionalityTypeProvider sdp = new SpatialDimensionalityTypeProvider();
		SpatialOptions so = sdp.createOptions();
		so.setCrs(CUSTOM_CRSCODE);
		PrimaryIndex primaryIndex = sdp.createPrimaryIndex(so);
		return primaryIndex;
	}

	public static final String S3_INPUT_PATH = "s3://geowave-test/data/gdelt";
	public static final String S3URL = "s3.amazonaws.com";

	public static boolean isYarn() {
		return VersionUtil.compareVersions(
				VersionInfo.getVersion(),
				"2.2.0") >= 0;
	}

	public static void testLocalIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String ingestFilePath,
			final int nthreads ) {
		testLocalIngest(
				dataStore,
				dimensionalityType,
				ingestFilePath,
				"geotools-vector",
				nthreads);

	}

	public static void testLocalIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String ingestFilePath ) {
		testLocalIngest(
				dataStore,
				dimensionalityType,
				ingestFilePath,
				"geotools-vector",
				1);
	}

	public static boolean isSet(
			final String str ) {
		return (str != null) && !str.isEmpty();
	}

	public static void deleteAll(
			final DataStorePluginOptions dataStore ) {
		dataStore.createDataStore().delete(
				new QueryOptions(),
				null);
	}

	public static void testLocalIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String ingestFilePath,
			final String format,
			final int nthreads ) {
		testLocalIngest(
				dataStore,
				dimensionalityType,
				null,
				ingestFilePath,
				format,
				nthreads);
	}

	public static void testLocalIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String crsCode,
			final String ingestFilePath,
			final String format,
			final int nthreads ) {

		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments

		// Ingest Formats
		final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin(format);

		// Indexes
		final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(
				",");
		final List<IndexPluginOptions> indexOptions = new ArrayList<IndexPluginOptions>(
				indexTypes.length);
		for (final String indexType : indexTypes) {
			final IndexPluginOptions indexOption = new IndexPluginOptions();
			indexOption.selectPlugin(indexType);
			if (crsCode != null) {
				((SpatialOptions) indexOption.getDimensionalityOptions()).setCrs(crsCode);
			}
			indexOptions.add(indexOption);
		}

		// Create the command and execute.
		final LocalToGeowaveCommand localIngester = new LocalToGeowaveCommand();
		localIngester.setPluginFormats(ingestFormatOptions);
		localIngester.setInputIndexOptions(indexOptions);
		localIngester.setInputStoreOptions(dataStore);
		localIngester.setParameters(
				ingestFilePath,
				null,
				null);
		localIngester.setThreads(nthreads);
		localIngester.execute(new ManualOperationParams());

		verifyStats(dataStore);

	}

	public static void testS3LocalIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String s3Url,
			final String ingestFilePath,
			final String format,
			final int nthreads )
			throws Exception {

		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments

		// Ingest Formats
		final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin(format);

		// Indexes
		final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(
				",");
		final List<IndexPluginOptions> indexOptions = new ArrayList<IndexPluginOptions>(
				indexTypes.length);
		for (final String indexType : indexTypes) {
			final IndexPluginOptions indexOption = new IndexPluginOptions();
			indexOption.selectPlugin(indexType);
			indexOptions.add(indexOption);
		}

		File configFile = File.createTempFile(
				"test_s3_local_ingest",
				null);
		ManualOperationParams operationParams = new ManualOperationParams();
		operationParams.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		final ConfigAWSCommand configS3 = new ConfigAWSCommand();
		configS3.setS3UrlParameter(s3Url);
		configS3.execute(operationParams);

		// Create the command and execute.
		final LocalToGeowaveCommand localIngester = new LocalToGeowaveCommand();
		localIngester.setPluginFormats(ingestFormatOptions);
		localIngester.setInputIndexOptions(indexOptions);
		localIngester.setInputStoreOptions(dataStore);
		localIngester.setParameters(
				ingestFilePath,
				null,
				null);
		localIngester.setThreads(nthreads);
		localIngester.execute(operationParams);

		verifyStats(dataStore);

	}

	public static void testSparkIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String format )
			throws Exception {
		testSparkIngest(
				dataStore,
				dimensionalityType,
				S3URL,
				S3_INPUT_PATH,
				format);

	}

	public static void testSparkIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String s3Url,
			final String ingestFilePath,
			final String format )
			throws Exception {

		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments

		// Indexes
		final String indexes = dimensionalityType.getDimensionalityArg();
		File configFile = File.createTempFile(
				"test_spark_ingest",
				null);
		ManualOperationParams operationParams = new ManualOperationParams();
		operationParams.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		final ConfigAWSCommand configS3 = new ConfigAWSCommand();
		configS3.setS3UrlParameter(s3Url);
		configS3.execute(operationParams);

		LocalInputCommandLineOptions localOptions = new LocalInputCommandLineOptions();
		localOptions.setFormats(format);

		SparkCommandLineOptions sparkOptions = new SparkCommandLineOptions();
		sparkOptions.setAppName("SparkIngestTest");
		sparkOptions.setMaster("local");
		sparkOptions.setHost("localhost");

		// Create the command and execute.
		final SparkIngestDriver sparkIngester = new SparkIngestDriver();
		Properties props = new Properties();
		dataStore.save(
				props,
				DataStorePluginOptions.getStoreNamespace("test"));
		final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(
				",");
		for (String indexType : indexTypes) {
			IndexPluginOptions pluginOptions = new IndexPluginOptions();
			pluginOptions.selectPlugin(indexType);
			pluginOptions.save(
					props,
					IndexPluginOptions.getIndexNamespace(indexType));
		}
		props.setProperty(
				ConfigAWSCommand.AWS_S3_ENDPOINT_URL,
				s3Url);
		sparkIngester.runOperation(
				null,
				localOptions,
				"test",
				indexes,
				new VisibilityOptions(),
				sparkOptions,
				ingestFilePath);

		verifyStats(dataStore);

	}

	private static void verifyStats(
			final DataStorePluginOptions dataStore ) {
		final ListStatsCommand listStats = new ListStatsCommand();
		listStats.setInputStoreOptions(dataStore);
		listStats.setParameters(
				null,
				null);
		try {
			listStats.execute(new ManualOperationParams());
		}
		catch (final ParameterException e) {
			throw new RuntimeException(
					e);
		}
	}

	public static long hashCentroid(
			final Geometry geometry ) {
		final Point centroid = geometry.getCentroid();
		return Double.doubleToLongBits(centroid.getX()) + Double.doubleToLongBits(centroid.getY() * 31);
	}

	public static class ExpectedResults
	{
		public Set<Long> hashedCentroids;
		public int count;

		@SuppressFBWarnings({
			"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"
		})
		public ExpectedResults(
				final Set<Long> hashedCentroids,
				final int count ) {
			this.hashedCentroids = hashedCentroids;
			this.count = count;
		}
	}

	public static ExpectedResults getExpectedResults(
			final CloseableIterator<?> results )
			throws IOException {
		final Set<Long> hashedCentroids = new HashSet<Long>();
		int expectedResultCount = 0;
		try {
			while (results.hasNext()) {
				final Object obj = results.next();
				if (obj instanceof SimpleFeature) {
					expectedResultCount++;
					final SimpleFeature feature = (SimpleFeature) obj;
					hashedCentroids.add(hashCentroid((Geometry) feature.getDefaultGeometry()));
				}
			}
		}
		finally {
			results.close();
		}
		return new ExpectedResults(
				hashedCentroids,
				expectedResultCount);
	}

	public static ExpectedResults getExpectedResults(
			final URL[] expectedResultsResources )
			throws IOException {
		return getExpectedResults(
				expectedResultsResources,
				null);
	}

	public static MathTransform transformFromCrs(
			CoordinateReferenceSystem crs ) {
		MathTransform mathTransform = null;
		if (crs != null) {
			try {
				mathTransform = CRS.findMathTransform(
						GeometryUtils.DEFAULT_CRS,
						crs,
						true);
			}
			catch (final FactoryException e) {
				LOGGER.warn(
						"Unable to create coordinate reference system transform",
						e);
			}
		}
		return mathTransform;
	}

	public static ExpectedResults getExpectedResults(
			final URL[] expectedResultsResources,
			CoordinateReferenceSystem crs )
			throws IOException {
		final Map<String, Object> map = new HashMap<String, Object>();
		DataStore dataStore = null;
		final Set<Long> hashedCentroids = new HashSet<Long>();
		int expectedResultCount = 0;
		MathTransform mathTransform = transformFromCrs(crs);
		for (final URL expectedResultsResource : expectedResultsResources) {
			map.put(
					"url",
					expectedResultsResource);
			SimpleFeatureIterator featureIterator = null;
			try {
				dataStore = DataStoreFinder.getDataStore(map);
				if (dataStore == null) {
					LOGGER.error("Could not get dataStore instance, getDataStore returned null");
					throw new IOException(
							"Could not get dataStore instance, getDataStore returned null");
				}
				final SimpleFeatureCollection expectedResults = dataStore.getFeatureSource(
						dataStore.getNames().get(
								0)).getFeatures();

				expectedResultCount += expectedResults.size();
				// unwrap the expected results into a set of features IDs so its
				// easy to check against
				featureIterator = expectedResults.features();
				while (featureIterator.hasNext()) {
					final SimpleFeature feature = featureIterator.next();
					Geometry geometry = (Geometry) feature.getDefaultGeometry();
					final long centroid = hashCentroid(mathTransform != null ? JTS.transform(
							geometry,
							mathTransform) : geometry);
					hashedCentroids.add(centroid);
				}
			}
			catch (MismatchedDimensionException | TransformException e) {
				LOGGER.warn(
						"Unable to transform geometry",
						e);
				Assert.fail("Unable to transform geometry to CRS: " + crs.toString());
			}
			finally {
				IOUtils.closeQuietly(featureIterator);
				if (dataStore != null) {
					dataStore.dispose();
				}
			}
		}
		return new ExpectedResults(
				hashedCentroids,
				expectedResultCount);
	}

	public static DistributableQuery resourceToQuery(
			final URL filterResource )
			throws IOException {
		return featureToQuery(resourceToFeature(filterResource));
	}

	public static SimpleFeature resourceToFeature(
			final URL filterResource )
			throws IOException {
		final Map<String, Object> map = new HashMap<String, Object>();
		DataStore dataStore = null;
		map.put(
				"url",
				filterResource);
		final SimpleFeature savedFilter;
		SimpleFeatureIterator sfi = null;
		try {
			dataStore = DataStoreFinder.getDataStore(map);
			if (dataStore == null) {
				LOGGER.error("Could not get dataStore instance, getDataStore returned null");
				throw new IOException(
						"Could not get dataStore instance, getDataStore returned null");
			}
			// just grab the first feature and use it as a filter
			sfi = dataStore.getFeatureSource(
					dataStore.getNames().get(
							0)).getFeatures().features();
			savedFilter = sfi.next();

		}
		finally {
			if (sfi != null) {
				sfi.close();
			}
			if (dataStore != null) {
				dataStore.dispose();
			}
		}
		return savedFilter;
	}

	protected static DistributableQuery featureToQuery(
			final SimpleFeature savedFilter ) {
		final Geometry filterGeometry = (Geometry) savedFilter.getDefaultGeometry();
		final Object startObj = savedFilter.getAttribute(TEST_FILTER_START_TIME_ATTRIBUTE_NAME);
		final Object endObj = savedFilter.getAttribute(TEST_FILTER_END_TIME_ATTRIBUTE_NAME);

		if ((startObj != null) && (endObj != null)) {
			// if we can resolve start and end times, make it a spatial temporal
			// query
			Date startDate = null, endDate = null;
			if (startObj instanceof Calendar) {
				startDate = ((Calendar) startObj).getTime();
			}
			else if (startObj instanceof Date) {
				startDate = (Date) startObj;
			}
			if (endObj instanceof Calendar) {
				endDate = ((Calendar) endObj).getTime();
			}
			else if (endObj instanceof Date) {
				endDate = (Date) endObj;
			}
			if ((startDate != null) && (endDate != null)) {
				return new SpatialTemporalQuery(
						startDate,
						endDate,
						filterGeometry);
			}
		}
		// otherwise just return a spatial query
		return new SpatialQuery(
				filterGeometry);
	}

	static protected void replaceParameters(
			final Map<String, String> values,
			final File file )
			throws IOException {
		{
			String str = FileUtils.readFileToString(file);
			for (final Entry<String, String> entry : values.entrySet()) {
				str = str.replaceAll(
						entry.getKey(),
						entry.getValue());
			}
			FileUtils.deleteQuietly(file);
			FileUtils.write(
					file,
					str);
		}
	}

	/**
	 * 
	 * @param testName
	 *            Name of the test that we are starting.
	 */
	public static void printStartOfTest(
			Logger LOGGER,
			String testName ) {
		// Format
		String paddedName = StringUtils.center(
				"STARTING " + testName,
				37);
		// Print
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* " + paddedName + " *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	/**
	 * 
	 * @param testName
	 *            Name of the test that we are starting.
	 * @param startMillis
	 *            The time (millis) that the test started.
	 */
	public static void printEndOfTest(
			Logger LOGGER,
			String testName,
			long startMillis ) {
		// Get Elapsed Time
		double elapsedS = (System.currentTimeMillis() - startMillis) / 1000.;
		// Format
		String paddedName = StringUtils.center(
				"FINISHED " + testName,
				37);
		String paddedElapsed = StringUtils.center(
				elapsedS + "s elapsed.",
				37);
		// Print
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* " + paddedName + " *");
		LOGGER.warn("* " + paddedElapsed + " *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	/**
	 *
	 * @param bi
	 *            sample
	 * @param ref
	 *            reference
	 * @param minPctError
	 *            used for testing subsampling - to ensure we are properly
	 *            subsampling we want there to be some error if subsampling is
	 *            aggressive (10 pixels)
	 * @param maxPctError
	 *            used for testing subsampling - we want to ensure at most we
	 *            are off by this percentile
	 */
	public static void testTileAgainstReference(
			final BufferedImage actual,
			final BufferedImage expected,
			final double minPctError,
			final double maxPctError ) {
		Assert.assertEquals(
				expected.getWidth(),
				actual.getWidth());
		Assert.assertEquals(
				expected.getHeight(),
				actual.getHeight());
		final int totalPixels = expected.getWidth() * expected.getHeight();
		final int minErrorPixels = (int) Math.round(minPctError * totalPixels);
		final int maxErrorPixels = (int) Math.round(maxPctError * totalPixels);
		int errorPixels = 0;
		// test under default style
		for (int x = 0; x < expected.getWidth(); x++) {
			for (int y = 0; y < expected.getHeight(); y++) {
				if (actual.getRGB(
						x,
						y) != expected.getRGB(
						x,
						y)) {
					errorPixels++;
					if (errorPixels > maxErrorPixels) {
						Assert.fail(String.format(
								"[%d,%d] failed to match ref=%d gen=%d",
								x,
								y,
								expected.getRGB(
										x,
										y),
								actual.getRGB(
										x,
										y)));
					}
				}
			}
		}
		if (errorPixels < minErrorPixels) {
			Assert
					.fail(String
							.format(
									"Subsampling did not work as expected; error pixels (%d) did not exceed the minimum threshold (%d)",
									errorPixels,
									minErrorPixels));
		}

		if (errorPixels > 0) {
			System.out.println(((float) errorPixels / (float) totalPixels) + "% pixels differed from expected");
		}
	}

	@Deprecated
	public static void assert200(
			String msg,
			int responseCode ) {
		Assert.assertEquals(
				msg,
				200,
				responseCode);
	}

	@Deprecated
	public static void assert400(
			String msg,
			int responseCode ) {
		Assert.assertEquals(
				msg,
				400,
				responseCode);
	}

	@Deprecated
	public static void assert404(
			String msg,
			int responseCode ) {
		Assert.assertEquals(
				msg,
				404,
				responseCode);
	}

	public static void assertStatusCode(
			String msg,
			int expectedCode,
			Response response ) {
		String assertionMsg = msg + String.format(
				": A %s response code should be received",
				expectedCode);
		Assert.assertEquals(
				assertionMsg,
				expectedCode,
				response.getStatus());
	}

	// Overload method with option to automatically generate assertion message.
	public static void assertStatusCode(
			int expectedCode,
			Response response ) {
		assertStatusCode(
				"REST call",
				expectedCode,
				response);
	}
}
