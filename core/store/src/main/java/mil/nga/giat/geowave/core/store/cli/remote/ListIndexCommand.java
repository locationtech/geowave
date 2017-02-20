package mil.nga.giat.geowave.core.store.cli.remote;

import java.io.File;
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
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.index.Index;

@GeowaveOperation(name = "listindex", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Display all indices in this remote store")
public class ListIndexCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	private DataStorePluginOptions inputStoreOptions = null;

	@Override
	public void execute(
			OperationParams params )
			throws IOException {
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		String inputStoreName = parameters.get(0);

		// Attempt to load store.
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

		final CloseableIterator<Index<?, ?>> it = inputStoreOptions.createIndexStore().getIndices();
		final StringBuffer buffer = new StringBuffer();
		while (it.hasNext()) {
			Index<?, ?> index = it.next();
			buffer.append(
					index.getId().getString()).append(
					' ');
		}
		it.close();

		JCommander.getConsole().println(
				"Available indexes: " + buffer.toString());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}
}
