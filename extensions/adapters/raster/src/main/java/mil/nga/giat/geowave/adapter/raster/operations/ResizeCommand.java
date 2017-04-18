package mil.nga.giat.geowave.adapter.raster.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.adapter.raster.operations.options.RasterTileResizeCommandLineOptions;
import mil.nga.giat.geowave.adapter.raster.resize.RasterTileResizeJobRunner;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "resize", parentOperation = RasterSection.class)
@Parameters(commandDescription = "Resize Raster Tiles")
public class ResizeCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<input store name> <output store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private RasterTileResizeCommandLineOptions options = new RasterTileResizeCommandLineOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	private DataStorePluginOptions outputStoreOptions = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		createRunner(
				params).runJob();
	}

	public RasterTileResizeJobRunner createRunner(
			OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <input store name> <output store name>");
		}

		String inputStoreName = parameters.get(0);
		String outputStoreName = parameters.get(1);

		// Config file
		File configFile = getGeoWaveConfigFile(params);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Attempt to load output store.
		if (outputStoreOptions == null) {
			StoreLoader outputStoreLoader = new StoreLoader(
					outputStoreName);
			if (!outputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + outputStoreLoader.getStoreName());
			}
			outputStoreOptions = outputStoreLoader.getDataStorePlugin();
		}

		RasterTileResizeJobRunner runner = new RasterTileResizeJobRunner(
				inputStoreOptions,
				outputStoreOptions,
				options);
		return runner;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String inputStore,
			String outputStore ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(inputStore);
		this.parameters.add(outputStore);
	}

	public RasterTileResizeCommandLineOptions getOptions() {
		return options;
	}

	public void setOptions(
			RasterTileResizeCommandLineOptions options ) {
		this.options = options;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	public DataStorePluginOptions getOutputStoreOptions() {
		return outputStoreOptions;
	}

	public void setOutputStoreOptions(
			DataStorePluginOptions outputStoreOptions ) {
		this.outputStoreOptions = outputStoreOptions;
	}
}
