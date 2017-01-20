package mil.nga.giat.geowave.analytic.mapreduce.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.analytic.mapreduce.kde.KDECommandLineOptions;
import mil.nga.giat.geowave.analytic.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "kde", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "Kernel Density Estimate")
public class KdeCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<input storename> <output storename>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private KDECommandLineOptions kdeOptions = new KDECommandLineOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	private DataStorePluginOptions outputStoreOptions = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		KDEJobRunner runner = createRunner(params);
		int status = runner.runJob();
		if (status != 0) {
			throw new RuntimeException(
					"Failed to execute: " + status);
		}
	}

	public KDEJobRunner createRunner(
			OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <input storename> <output storename>");
		}

		String inputStore = parameters.get(0);
		String outputStore = parameters.get(1);
		// Config file
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					inputStore);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Attempt to load output store.
		if (outputStoreOptions == null) {
			StoreLoader outputStoreLoader = new StoreLoader(
					outputStore);
			if (!outputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + outputStoreLoader.getStoreName());
			}
			outputStoreOptions = outputStoreLoader.getDataStorePlugin();
		}

		KDEJobRunner runner = new KDEJobRunner(
				kdeOptions,
				inputStoreOptions,
				outputStoreOptions);
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

	public KDECommandLineOptions getKdeOptions() {
		return kdeOptions;
	}

	public void setKdeOptions(
			KDECommandLineOptions kdeOptions ) {
		this.kdeOptions = kdeOptions;
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
