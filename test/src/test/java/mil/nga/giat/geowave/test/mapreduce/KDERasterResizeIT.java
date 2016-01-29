package mil.nga.giat.geowave.test.mapreduce;

import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Map.Entry;

import javax.media.jai.Interpolation;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.geometry.GeneralEnvelope;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.coverage.grid.GridCoverage;

import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveRasterConfig;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveRasterReader;
import mil.nga.giat.geowave.adapter.raster.resize.RasterTileResizeCommandLineOptions;
import mil.nga.giat.geowave.adapter.raster.resize.RasterTileResizeJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.kde.KDECommandLineOptions;
import mil.nga.giat.geowave.analytic.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

public class KDERasterResizeIT extends
		MapReduceTestEnvironment
{
	private static final String TEST_COVERAGE_NAME_PREFIX = "TEST_COVERAGE";
	private static final String TEST_RESIZE_COVERAGE_NAME_PREFIX = "TEST_RESIZE";
	private static final String TEST_COVERAGE_NAMESPACE = "mil_nga_giat_geowave_test_coverage";
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "kde-testdata.zip";
	protected static final String KDE_INPUT_DIR = TEST_CASE_BASE + "kde_test_case/";
	private static final String KDE_SHAPEFILE_FILE = KDE_INPUT_DIR + "kde-test.shp";
	private static final double TARGET_MIN_LON = 155;
	private static final double TARGET_MIN_LAT = 16;
	private static final double TARGET_DECIMAL_DEGREES_SIZE = 0.132;
	private static final String KDE_FEATURE_TYPE_NAME = "kde-test";
	private static final int MIN_TILE_SIZE_POWER_OF_2 = 0;
	private static final int MAX_TILE_SIZE_POWER_OF_2 = 6;
	private static final int INCREMENT = 2;
	private static final int BASE_MIN_LEVEL = 15;
	private static final int BASE_MAX_LEVEL = 17;

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		GeoWaveTestEnvironment.unZipFile(
				new File(
						KDERasterResizeIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TEST_CASE_BASE);
	}

	@Test
	public void testKDEAndRasterResize()
			throws Exception {
		accumuloOperations.deleteAll();
		testLocalIngest(
				DimensionalityType.SPATIAL,
				KDE_SHAPEFILE_FILE);
		// use the min level to define the request boundary because it is the
		// most coarse grain
		final double decimalDegreesPerCellMinLevel = 180.0 / Math.pow(
				2,
				BASE_MIN_LEVEL);
		final double cellOriginXMinLevel = Math.round(TARGET_MIN_LON / decimalDegreesPerCellMinLevel);
		final double cellOriginYMinLevel = Math.round(TARGET_MIN_LAT / decimalDegreesPerCellMinLevel);
		final double numCellsMinLevel = Math.round(TARGET_DECIMAL_DEGREES_SIZE / decimalDegreesPerCellMinLevel);
		final GeneralEnvelope queryEnvelope = new GeneralEnvelope(
				new double[] {
					// this is exactly on a tile boundary, so there will be no
					// scaling on the tile composition/rendering
					decimalDegreesPerCellMinLevel * cellOriginXMinLevel,
					decimalDegreesPerCellMinLevel * cellOriginYMinLevel
				},
				new double[] {
					// these values are also on a tile boundary, to avoid
					// scaling
					decimalDegreesPerCellMinLevel * (cellOriginXMinLevel + numCellsMinLevel),
					decimalDegreesPerCellMinLevel * (cellOriginYMinLevel + numCellsMinLevel)
				});

		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String tileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			ToolRunner.run(
					new KDEJobRunner(),
					new String[] {
						"-" + KDECommandLineOptions.FEATURE_TYPE_KEY,
						KDE_FEATURE_TYPE_NAME,
						"-" + KDECommandLineOptions.MIN_LEVEL_KEY,
						new Integer(
								BASE_MIN_LEVEL - i).toString(),
						"-" + KDECommandLineOptions.MAX_LEVEL_KEY,
						new Integer(
								BASE_MAX_LEVEL - i).toString(),
						"-" + KDECommandLineOptions.MIN_SPLITS_KEY,
						new Integer(
								MIN_INPUT_SPLITS).toString(),
						"-" + KDECommandLineOptions.MAX_SPLITS_KEY,
						new Integer(
								MAX_INPUT_SPLITS).toString(),
						"-" + KDECommandLineOptions.COVERAGE_NAME_KEY,
						tileSizeCoverageName,
						"-" + KDECommandLineOptions.HDFS_HOST_PORT_KEY,
						hdfs,
						"-" + KDECommandLineOptions.JOB_TRACKER_HOST_PORT_KEY,
						jobtracker,
						"-" + KDECommandLineOptions.TILE_SIZE_KEY,
						new Integer(
								(int) Math.pow(
										2,
										i)).toString(),
						"-input_" + GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY,
						TEST_NAMESPACE,
						"-input_datastore",
						"accumulo",
						"-input_" + GenericStoreCommandLineOptions.CONNECTION_OPTION_KEY,
						BasicAccumuloOperations.ZOOKEEPER_CONFIG_NAME + "=" + zookeeper + ";" + BasicAccumuloOperations.INSTANCE_CONFIG_NAME + "=" + accumuloInstance + ";" + BasicAccumuloOperations.USER_CONFIG_NAME + "=" + accumuloUser + ";" + BasicAccumuloOperations.PASSWORD_CONFIG_NAME + "=" + accumuloPassword,
						"-output_" + GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY,
						TEST_COVERAGE_NAMESPACE,
						"-output_datastore",
						"accumulo",
						"-output_" + GenericStoreCommandLineOptions.CONNECTION_OPTION_KEY,
						BasicAccumuloOperations.ZOOKEEPER_CONFIG_NAME + "=" + zookeeper + ";" + BasicAccumuloOperations.INSTANCE_CONFIG_NAME + "=" + accumuloInstance + ";" + BasicAccumuloOperations.USER_CONFIG_NAME + "=" + accumuloUser + ";" + BasicAccumuloOperations.PASSWORD_CONFIG_NAME + "=" + accumuloPassword
					});
		}
		final int numLevels = (BASE_MAX_LEVEL - BASE_MIN_LEVEL) + 1;
		final double[][][][] initialSampleValuesPerRequestSize = new double[numLevels][][][];
		for (int l = 0; l < numLevels; l++) {
			initialSampleValuesPerRequestSize[l] = testSamplesMatch(
					TEST_COVERAGE_NAME_PREFIX,
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					queryEnvelope,
					new Rectangle(
							(int) (numCellsMinLevel * Math.pow(
									2,
									l)),
							(int) (numCellsMinLevel * Math.pow(
									2,
									l))),
					null);
		}

		final Connector conn = ConnectorPool.getInstance().getConnector(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword);
		conn.tableOperations().compact(
				TEST_COVERAGE_NAMESPACE + "_" + DEFAULT_ALLTIER_SPATIAL_INDEX.getId().getString(),
				null,
				null,
				true,
				true);
		for (int l = 0; l < numLevels; l++) {
			testSamplesMatch(
					TEST_COVERAGE_NAME_PREFIX,
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					queryEnvelope,
					new Rectangle(
							(int) (numCellsMinLevel * Math.pow(
									2,
									l)),
							(int) (numCellsMinLevel * Math.pow(
									2,
									l))),
					initialSampleValuesPerRequestSize[l]);
		}
		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String originalTileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			final String resizeTileSizeCoverageName = TEST_RESIZE_COVERAGE_NAME_PREFIX + i;
			ToolRunner.run(
					new RasterTileResizeJobRunner(),
					new String[] {
						"-" + RasterTileResizeCommandLineOptions.INPUT_COVERAGE_NAME_KEY,
						originalTileSizeCoverageName,
						"-" + RasterTileResizeCommandLineOptions.MIN_SPLITS_KEY,
						new Integer(
								MIN_INPUT_SPLITS).toString(),
						"-" + RasterTileResizeCommandLineOptions.MAX_SPLITS_KEY,
						new Integer(
								MAX_INPUT_SPLITS).toString(),
						"-" + RasterTileResizeCommandLineOptions.HDFS_HOST_PORT_KEY,
						hdfs,
						"-" + RasterTileResizeCommandLineOptions.JOB_TRACKER_HOST_PORT_KEY,
						jobtracker,
						"-" + RasterTileResizeCommandLineOptions.OUTPUT_COVERAGE_NAME_KEY,
						resizeTileSizeCoverageName,
						"-" + RasterTileResizeCommandLineOptions.INDEX_ID_KEY,
						DEFAULT_ALLTIER_SPATIAL_INDEX.getId().getString(),
						"-" + RasterTileResizeCommandLineOptions.TILE_SIZE_KEY,
						new Integer(
								(int) Math.pow(
										2,
										MAX_TILE_SIZE_POWER_OF_2 - i)).toString(),
						"-input_" + GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY,
						TEST_COVERAGE_NAMESPACE,
						"-input_datastore",
						"accumulo",
						"-input_" + GenericStoreCommandLineOptions.CONNECTION_OPTION_KEY,
						BasicAccumuloOperations.ZOOKEEPER_CONFIG_NAME + "=" + zookeeper + ";" + BasicAccumuloOperations.INSTANCE_CONFIG_NAME + "=" + accumuloInstance + ";" + BasicAccumuloOperations.USER_CONFIG_NAME + "=" + accumuloUser + ";" + BasicAccumuloOperations.PASSWORD_CONFIG_NAME + "=" + accumuloPassword,
						"-output_" + GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY,
						TEST_COVERAGE_NAMESPACE,
						"-output_datastore",
						"accumulo",
						"-output_" + GenericStoreCommandLineOptions.CONNECTION_OPTION_KEY,
						BasicAccumuloOperations.ZOOKEEPER_CONFIG_NAME + "=" + zookeeper + ";" + BasicAccumuloOperations.INSTANCE_CONFIG_NAME + "=" + accumuloInstance + ";" + BasicAccumuloOperations.USER_CONFIG_NAME + "=" + accumuloUser + ";" + BasicAccumuloOperations.PASSWORD_CONFIG_NAME + "=" + accumuloPassword
					});
		}

		for (int l = 0; l < numLevels; l++) {
			testSamplesMatch(
					TEST_RESIZE_COVERAGE_NAME_PREFIX,
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					queryEnvelope,
					new Rectangle(
							(int) (numCellsMinLevel * Math.pow(
									2,
									l)),
							(int) (numCellsMinLevel * Math.pow(
									2,
									l))),
					initialSampleValuesPerRequestSize[l]);
		}

		conn.tableOperations().compact(
				TEST_COVERAGE_NAMESPACE + "_" + DEFAULT_ALLTIER_SPATIAL_INDEX.getId().getString(),
				null,
				null,
				true,
				true);
		for (int l = 0; l < numLevels; l++) {
			testSamplesMatch(
					TEST_RESIZE_COVERAGE_NAME_PREFIX,
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					queryEnvelope,
					new Rectangle(
							(int) (numCellsMinLevel * Math.pow(
									2,
									l)),
							(int) (numCellsMinLevel * Math.pow(
									2,
									l))),
					initialSampleValuesPerRequestSize[l]);
		}
	}

	private static double[][][] testSamplesMatch(
			final String coverageNamePrefix,
			final int numCoverages,
			final GeneralEnvelope queryEnvelope,
			final Rectangle pixelDimensions,
			double[][][] expectedResults )
			throws Exception {
		final Map<String, String> options = getAccumuloConfigOptions();
		final StringBuilder str = new StringBuilder(
				GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY).append(
				"=").append(
				TEST_COVERAGE_NAMESPACE).append(
				";equalizeHistogramOverride=false;interpolationOverride=").append(
				Interpolation.INTERP_NEAREST);

		for (final Entry<String, String> entry : options.entrySet()) {
			str.append(
					";").append(
					entry.getKey()).append(
					"=").append(
					entry.getValue());
		}

		final GeoWaveRasterReader reader = new GeoWaveRasterReader(
				GeoWaveRasterConfig.readFromConfigParams(str.toString()));

		queryEnvelope.setCoordinateReferenceSystem(GeoWaveGTRasterFormat.DEFAULT_CRS);
		final Raster[] rasters = new Raster[numCoverages];
		int coverageCount = 0;
		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String tileSizeCoverageName = coverageNamePrefix + i;
			final GridCoverage gridCoverage = reader.renderGridCoverage(
					tileSizeCoverageName,
					pixelDimensions,
					queryEnvelope,
					null,
					null);
			final RenderedImage image = gridCoverage.getRenderedImage();
			final Raster raster = image.getData();
			rasters[coverageCount++] = raster;
		}
		for (int i = 0; i < numCoverages; i++) {
			final boolean initialResults = expectedResults == null;
			if (initialResults) {
				expectedResults = new double[rasters[i].getWidth()][rasters[i].getHeight()][rasters[i].getNumBands()];
			}
			else {
				Assert.assertEquals(
						"The expected width does not match the expected width for the coverage " + i,
						expectedResults.length,
						rasters[i].getWidth());
				Assert.assertEquals(
						"The expected height does not match the expected height for the coverage " + i,
						expectedResults[0].length,
						rasters[i].getHeight());
				Assert.assertEquals(
						"The expected number of bands does not match the expected bands for the coverage " + i,
						expectedResults[0][0].length,
						rasters[i].getNumBands());
			}
			for (int x = 0; x < rasters[i].getWidth(); x++) {
				for (int y = 0; y < rasters[i].getHeight(); y++) {
					for (int b = 0; b < rasters[i].getNumBands(); b++) {
						final double sample = rasters[i].getSampleDouble(
								x,
								y,
								b);
						if (initialResults) {
							expectedResults[x][y][b] = sample;
						}
						else {
							Assert.assertEquals(
									"The sample does not match the expected sample value for the coverage " + i + " at x=" + x + ",y=" + y + ",b=" + b,
									new Double(
											expectedResults[x][y][b]),
									new Double(
											sample));
						}
					}
				}
			}
		}
		return expectedResults;
	}
}
