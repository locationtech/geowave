package mil.nga.giat.geowave.core.store.operations.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "listindex", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Display all indices in this remote store")
public class ListIndexCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params )
			throws IOException {

		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		String inputStoreName = parameters.get(0);

		// Attempt to load the desired input store

		String result;

		StoreLoader inputStoreLoader = new StoreLoader(
				inputStoreName);
		if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
			result = "Cannot find store name: " + inputStoreLoader.getStoreName();
		}
		else {

			// Now that store is loaded, pull the list of indexes

			DataStorePluginOptions inputStoreOptions = inputStoreLoader.getDataStorePlugin();

			final CloseableIterator<Index<?, ?>> it = inputStoreOptions.createIndexStore().getIndices();
			final StringBuffer buffer = new StringBuffer();
			while (it.hasNext()) {
				Index<?, ?> index = it.next();
				buffer.append(
						index.getId().getString()).append(
						' ');
			}
			it.close();
			result = "Available indexes: " + buffer.toString();
		}

		JCommander.getConsole().println(
				result);
	}

}
