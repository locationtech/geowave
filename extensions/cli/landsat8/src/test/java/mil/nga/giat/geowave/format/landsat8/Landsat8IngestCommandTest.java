package mil.nga.giat.geowave.format.landsat8;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import mil.gma.giat.geowave.gdal.InstallGdal;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.operations.config.AddIndexCommand;
import mil.nga.giat.geowave.core.store.operations.config.AddStoreCommand;

public class Landsat8IngestCommandTest {
	
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	
	private File configFile;
	
	@Before
	public void setup() throws IOException {
		// This needs to write to someplace on the path (like /usr/lib), which requires sudo.
		// Right now I needed to copy it there myself.
		InstallGdal.main(new String[]{});
		
		configFile = tempFolder.newFile();
		setupStore();
		setupIndex();
	}
	
	private void setupStore() throws IOException {
		AddStoreCommand command = new AddStoreCommand();
		OperationParams params = new ManualOperationParams();
		
		command.setStoreType("memory");
		command.getParameters().add("teststore");
		params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
		
		command.prepare(params);
		command.execute(params);
	}
	
	private void setupIndex() {
		AddIndexCommand command = new AddIndexCommand();
		OperationParams params = new ManualOperationParams();
		
		command.setType("spatial");
		command.getParameters().add("testindex");
		params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
		
		command.prepare(params);
		command.execute(params);
	}


	@Test
	public void testExecute() throws Exception {
		Landsat8IngestCommand command = new Landsat8IngestCommand();
		OperationParams params = new ManualOperationParams();
		
		command.analyzeOptions.setWorkspaceDir(tempFolder.newFolder().getAbsolutePath());
		command.analyzeOptions.setUseCachedScenes(true);
		command.analyzeOptions.setNBestScenes(1);
		command.analyzeOptions.setCqlFilter("BBOX(shape,-76.6,42.34,-76.4,42.54) and band='BQA'");
		command.parameters.add("teststore");
		command.parameters.add("testindex");
		params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
		
		command.execute(params);
	}

}
