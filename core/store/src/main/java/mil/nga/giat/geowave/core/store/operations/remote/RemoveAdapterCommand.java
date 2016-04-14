package mil.nga.giat.geowave.core.store.operations.remote;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "rmadapter", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Remove an adapter from the remote store and all associated data for the adapter")
public class RemoveAdapterCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<store name> <adapterId>")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <store name> <adapterId>");
		}

		String storeName = parameters.get(0);
		// String adapterId = parameters.get(1);

		// Attempt to load store.
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		StoreLoader storeOptions = new StoreLoader(
				storeName);
		if (!storeOptions.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + storeOptions.getStoreName());
		}

		// Get Adapter Store
		throw new UnsupportedOperationException(
				"This operation is not currently supported");

	}
}
