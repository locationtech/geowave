package mil.nga.giat.geowave.test.landsat;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.imageio.ImageIO;
import javax.media.jai.Interpolation;
import javax.media.jai.PlanarImage;

import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveRasterConfig;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveRasterReader;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.format.landsat8.BandFeatureIterator;
import mil.nga.giat.geowave.format.landsat8.Landsat8BasicCommandLineOptions;
import mil.nga.giat.geowave.format.landsat8.Landsat8DownloadCommandLineOptions;
import mil.nga.giat.geowave.format.landsat8.Landsat8RasterIngestCommandLineOptions;
import mil.nga.giat.geowave.format.landsat8.RasterIngestRunner;
import mil.nga.giat.geowave.format.landsat8.SceneFeatureIterator;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.referencing.operation.projection.MapProjection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import freemarker.template.Configuration;
import freemarker.template.Template;

@RunWith(GeoWaveITRunner.class)
public class LandsatIT
{
	private static class RasterIngestTester extends
			RasterIngestRunner
	{
		DataStorePluginOptions dataStoreOptions;

		public RasterIngestTester(
				final DataStorePluginOptions dataStoreOptions,
				final Landsat8BasicCommandLineOptions analyzeOptions,
				final Landsat8DownloadCommandLineOptions downloadOptions,
				final Landsat8RasterIngestCommandLineOptions ingestOptions,
				final List<String> parameters ) {
			super(
					analyzeOptions,
					downloadOptions,
					ingestOptions,
					parameters);
			this.dataStoreOptions = dataStoreOptions;
		}

		@Override
		protected void runInternal(
				final OperationParams params )
				throws Exception {
			// TODO Auto-generated method stub
			super.runInternal(params);
		}

		@Override
		protected void processParameters(
				final OperationParams params )
				throws Exception {
			store = dataStoreOptions.createDataStore();
			indices = new PrimaryIndex[] {
				new SpatialIndexBuilder().createIndex()
			};
			coverageNameTemplate = new Template(
					"name",
					new StringReader(
							ingestOptions.getCoverageName()),
					new Configuration());
		}

	}

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStoreOptions;
	private static final String REFERENCE_LANDSAT_IMAGE_PATH = "src/test/resources/landsat/expected.png";
	private static final int MIN_PATH = 198;
	private static final int MAX_PATH = 199;
	private static final int MIN_ROW = 36;
	private static final int MAX_ROW = 37;
	private static final double WEST = -2.2;
	private static final double EAST = -1.4;
	private static final double NORTH = 34.25;
	private static final double SOUTH = 33.5;

	private final static Logger LOGGER = LoggerFactory.getLogger(LandsatIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING LandsatIT             *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED LandsatIT               *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testMosaic()
			throws Exception {
		MapProjection.SKIP_SANITY_CHECKS = true;
		TestUtils.deleteAll(dataStoreOptions);
		// just use the QA band as QA is the smallest, get the best cloud cover,
		// but ensure it is before now so no recent collection affects the test
		final Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();
		analyzeOptions
				.setCqlFilter(String
						.format(
								"BBOX(%s,%f,%f,%f,%f) AND %s='B4' AND %s <= '%s' AND path >= %d AND path <= %d AND row >= %d AND row <= %d",
								SceneFeatureIterator.SHAPE_ATTRIBUTE_NAME,
								WEST,
								SOUTH,
								EAST,
								NORTH,
								BandFeatureIterator.BAND_ATTRIBUTE_NAME,
								SceneFeatureIterator.ACQUISITION_DATE_ATTRIBUTE_NAME,
								"2016-06-01T00:00:00Z",
								MIN_PATH,
								MAX_PATH,
								MIN_ROW,
								MAX_ROW));
		analyzeOptions.setNBestPerSpatial(true);
		analyzeOptions.setNBestScenes(1);
		analyzeOptions.setUseCachedScenes(true);
		final Landsat8DownloadCommandLineOptions downloadOptions = new Landsat8DownloadCommandLineOptions();
		final Landsat8RasterIngestCommandLineOptions ingestOptions = new Landsat8RasterIngestCommandLineOptions();
		ingestOptions.setRetainImages(true);
		ingestOptions.setCreatePyramid(true);
		ingestOptions.setCreateHistogram(true);
		ingestOptions.setCoverageName("test");
		// crop to the specified bbox
		ingestOptions.setCropToSpatialConstraint(true);
		final RasterIngestTester runner = new RasterIngestTester(
				dataStoreOptions,
				analyzeOptions,
				downloadOptions,
				ingestOptions,
				null);
		runner.runInternal(null);

		final StringBuilder str = new StringBuilder(
				StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION).append(
				"=").append(
				dataStoreOptions.getGeowaveNamespace()).append(
				";equalizeHistogramOverride=false;interpolationOverride=").append(
				Interpolation.INTERP_NEAREST);

		str.append(
				";").append(
				GeoWaveStoreFinder.STORE_HINT_KEY).append(
				"=").append(
				dataStoreOptions.getType());

		final Map<String, String> options = dataStoreOptions.getOptionsAsMap();

		for (final Entry<String, String> entry : options.entrySet()) {
			if (!entry.getKey().equals(
					StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION)) {
				str.append(
						";").append(
						entry.getKey()).append(
						"=").append(
						entry.getValue());
			}
		}
		final GeneralEnvelope queryEnvelope = new GeneralEnvelope(
				new double[] {
					WEST,
					SOUTH
				},
				new double[] {
					EAST,
					NORTH
				});
		queryEnvelope.setCoordinateReferenceSystem(GeoWaveGTRasterFormat.DEFAULT_CRS);

		final GeoWaveRasterReader reader = new GeoWaveRasterReader(
				GeoWaveRasterConfig.readFromConfigParams(str.toString()));
		final GridCoverage2D gridCoverage = reader.renderGridCoverage(
				"test",
				new Rectangle(
						0,
						0,
						1024,
						1024),
				queryEnvelope,
				null,
				null,
				null);
		final RenderedImage result = gridCoverage.getRenderedImage();

		// test the result with expected, allowing for minimal error
		final BufferedImage reference = ImageIO.read(new File(
				REFERENCE_LANDSAT_IMAGE_PATH));
		TestUtils.testTileAgainstReference(
				PlanarImage.wrapRenderedImage(
						result).getAsBufferedImage(),
				reference,
				0,
				0.005);
		MapProjection.SKIP_SANITY_CHECKS = false;
	}
}
