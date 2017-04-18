package mil.nga.giat.geowave.core.store.operations.remote;

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
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.AdapterIdQuery;
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
			OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <store name> <adapterId>");
		}

		String inputStoreName = parameters.get(0);
		String adapterId = parameters.get(1);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		LOGGER.info("Deleting everything in store: " + inputStoreName + " with adapter id: " + adapterId);

		inputStoreOptions.createDataStore().delete(
				new QueryOptions(),
				new AdapterIdQuery(
						new ByteArrayId(
								adapterId)));

	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName,
			String adapterId ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
		this.parameters.add(adapterId);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

}
