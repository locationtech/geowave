package mil.nga.giat.geowave.core.store.cli.remote;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

@GeowaveOperation(name = "rmadapter", parentOperation = RemoteSection.class)
@Parameters(hidden = true, commandDescription = "Remove an adapter from the remote store and all associated data for the adapter")
public class RemoveAdapterCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RemoveAdapterCommand.class);

	@Parameter(description = "<store name> <adapterId>")
	private List<String> parameters = new ArrayList<String>();

	private DataStorePluginOptions inputStoreOptions = null;

	@Override
	public void execute(
			final OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <store name> <adapterId>");
		}

		final String inputStoreName = parameters.get(0);
		final String adapterId = parameters.get(1);

		// Attempt to load store.
		final File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		LOGGER.info("Deleting everything in store: " + inputStoreName + " with adapter id: " + adapterId);
		final QueryOptions options = new QueryOptions();
		options.setAdapterId(new ByteArrayId(
				adapterId));
		inputStoreOptions.createDataStore().delete(
				options,
				new EverythingQuery());

	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName,
			final String adapterId ) {
		parameters = new ArrayList<String>();
		parameters.add(storeName);
		parameters.add(adapterId);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

}
