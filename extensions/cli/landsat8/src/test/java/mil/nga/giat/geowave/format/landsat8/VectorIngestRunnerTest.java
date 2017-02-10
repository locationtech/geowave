package mil.nga.giat.geowave.format.landsat8;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import com.beust.jcommander.ParameterException;

import it.geosolutions.jaiext.JAIExt;
import mil.nga.giat.geowave.adapter.raster.plugin.gdal.InstallGdal;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;

public class VectorIngestRunnerTest
{

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
	public void testIngest()
			throws Exception {
		JAIExt.initJAIEXT();

		Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();
		analyzeOptions.setNBestScenes(1);
		analyzeOptions.setCqlFilter("BBOX(shape,-76.6,42.34,-76.4,42.54) and band='BQA' and sizeMB < 1");
		analyzeOptions.setUseCachedScenes(true);
		analyzeOptions.setWorkspaceDir(Tests.WORKSPACE_DIR);

		Landsat8DownloadCommandLineOptions downloadOptions = new Landsat8DownloadCommandLineOptions();
		downloadOptions.setOverwriteIfExists(false);

		Landsat8RasterIngestCommandLineOptions ingestOptions = new Landsat8RasterIngestCommandLineOptions();
		ingestOptions.setRetainImages(true);
		ingestOptions.setCreatePyramid(true);
		ingestOptions.setCreateHistogram(true);
		VectorIngestRunner runner = new VectorIngestRunner(
				analyzeOptions,
				Arrays.asList(
						"memorystore",
						"spatialindex"));
		ManualOperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				new File(
						VectorIngestRunnerTest.class.getClassLoader().getResource(
								"geowave-config.properties").toURI()));
		runner.runInternal(params);
		try (CloseableIterator<Object> results = getStore(
				params).query(
				new QueryOptions(),
				new EverythingQuery())) {
			assertTrue(
					"Store is not empty",
					results.hasNext());
		}

		// Not sure what assertions can be made about the index.
	}

	private DataStore getStore(
			OperationParams params ) {
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		StoreLoader inputStoreLoader = new StoreLoader(
				"memorystore");
		if (!inputStoreLoader.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}
		DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();
		return storeOptions.createDataStore();
	}

	/*
	 * private PrimaryIndex getIndex(OperationParams params){ File configFile =
	 * (File) params.getContext().get( ConfigOptions.PROPERTIES_FILE_CONTEXT);
	 * IndexLoader indexLoader = new IndexLoader("spatialindex"); if
	 * (!indexLoader.loadFromConfig(configFile)) { throw new ParameterException(
	 * "Cannot find index(s) by name: " + indexLoader.getIndexName()); }
	 * 
	 * IndexPluginOptions indexOptions =
	 * Iterables.getOnlyElement(indexLoader.getLoadedIndexes()); return
	 * indexOptions.createPrimaryIndex(); }
	 */
}
