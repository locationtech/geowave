package mil.nga.giat.geowave.adapter.vector.export;

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
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "mrexport", parentOperation = VectorSection.class)
@Parameters(commandDescription = "")
public class VectorMRExportCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private VectorMRExportOptions mrOptions = new VectorMRExportOptions();

	private DataStorePluginOptions storeOptions = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		createRunner(
				params).runJob();
	}

	public VectorMRExportJobRunner createRunner(
			OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <store name>");
		}

		String storeName = parameters.get(0);

		// Config file
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load store.
		if (storeOptions == null) {
			StoreLoader storeLoader = new StoreLoader(
					storeName);
			if (!storeLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + storeLoader.getStoreName());
			}
			storeOptions = storeLoader.getDataStorePlugin();
		}

		VectorMRExportJobRunner vectorRunner = new VectorMRExportJobRunner(
				storeOptions,
				mrOptions);
		return vectorRunner;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}

	public VectorMRExportOptions getMrOptions() {
		return mrOptions;
	}

	public void setMrOptions(
			VectorMRExportOptions mrOptions ) {
		this.mrOptions = mrOptions;
	}

	public DataStorePluginOptions getStoreOptions() {
		return storeOptions;
	}

	public void setStoreOptions(
			DataStorePluginOptions storeOptions ) {
		this.storeOptions = storeOptions;
	}
}