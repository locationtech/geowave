package mil.nga.giat.geowave.mapreduce.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.RemoteSection;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.mapreduce.copy.StoreCopyJobRunner;

@GeowaveOperation(name = "copy", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Copy a data store")
public class CopyCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<input store name> <output store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private CopyCommandOptions options = new CopyCommandOptions();

	private DataStorePluginOptions inputStoreOptions = null;
	private DataStorePluginOptions outputStoreOptions = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		createRunner(
				params).runJob();
	}

	public StoreCopyJobRunner createRunner(
			OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <input store name> <output store name>");
		}

		String inputStoreName = parameters.get(0);
		String outputStoreName = parameters.get(1);

		// Config file
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

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

		String jobName = "Copy " + inputStoreName + " to " + outputStoreName;

		StoreCopyJobRunner runner = new StoreCopyJobRunner(
				inputStoreOptions,
				outputStoreOptions,
				options,
				jobName);

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

	public CopyCommandOptions getOptions() {
		return options;
	}

	public void setOptions(
			CopyCommandOptions options ) {
		this.options = options;
	}
}
