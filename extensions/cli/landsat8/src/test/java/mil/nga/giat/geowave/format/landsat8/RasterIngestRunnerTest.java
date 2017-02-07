package mil.nga.giat.geowave.format.landsat8;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.geosolutions.jaiext.JAIExt;
import mil.nga.giat.geowave.adapter.raster.plugin.gdal.InstallGdal;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;

public class RasterIngestRunnerTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RasterIngestRunnerTest.class);

	@BeforeClass
	public static void setup()
			throws IOException {
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
		InstallGdal.main(new String[] {
			System.getenv("GDAL_DIR")
		});
	}

	@Test
	public void testIngest() {
		JAIExt.initJAIEXT();
		// TODO setup ingest runner test
		final Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();
		analyzeOptions.setNBestScenes(1);
		analyzeOptions.setCqlFilter("BBOX(shape,-76.6,42.34,-76.4,42.54) and band='BQA'");
		analyzeOptions.setUseCachedScenes(true);
		// use the same workspace directory as the ITs to consolidate what is
		// downloaded
		analyzeOptions.setWorkspaceDir("../../../test/landsat8");
		final Landsat8DownloadCommandLineOptions downloadOptions = new Landsat8DownloadCommandLineOptions();
		downloadOptions.setOverwriteIfExists(false);
		final Landsat8RasterIngestCommandLineOptions ingestOptions = new Landsat8RasterIngestCommandLineOptions();
		ingestOptions.setRetainImages(true);
		ingestOptions.setCreatePyramid(true);
		ingestOptions.setCreateHistogram(true);
		final RasterIngestRunner runner = new RasterIngestRunner(
				analyzeOptions,
				downloadOptions,
				ingestOptions,
				Arrays.asList(
						"memorystore",
						"spatialindex"));
		final ManualOperationParams params = new ManualOperationParams();

		try {
			params.getContext().put(
					ConfigOptions.PROPERTIES_FILE_CONTEXT,
					new File(
							RasterIngestRunnerTest.class.getClassLoader().getResource(
									"geowave-config.properties").toURI()));
			runner.runInternal(params);
		}
		catch (final Exception e) {
			LOGGER.error(
					"unable to run operation",
					e);
			Assert.fail(e.getMessage());
		}
	}
}
