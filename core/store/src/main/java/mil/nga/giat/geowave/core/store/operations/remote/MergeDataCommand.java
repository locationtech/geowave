package mil.nga.giat.geowave.core.store.operations.remote;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "mergedata", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Merge all rows for a given adapter and index")
public class MergeDataCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<storename> <indexname>")
	private List<String> parameters = new ArrayList<String>();

	private DataStorePluginOptions inputStoreOptions = null;

	private List<IndexPluginOptions> inputIndexOptions = null;

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <storename> <indexname>");
		}

		final String inputStoreName = parameters.get(0);
		final String indexList = parameters.get(1);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		} // Load the Indexes
		if (inputIndexOptions == null) {
			final IndexLoader indexLoader = new IndexLoader(
					indexList);
			if (!indexLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + indexList);
			}
			inputIndexOptions = indexLoader.getLoadedIndexes();
		}
		final AdapterStore adapterStore = inputStoreOptions.createAdapterStore();
		final AdapterIndexMappingStore adapterIndexMappingStore = inputStoreOptions.createAdapterIndexMappingStore();
		final DataStoreOperations operations = inputStoreOptions.createDataStoreOperations();

		for (final IndexPluginOptions i : inputIndexOptions) {
			final PrimaryIndex index = i.createPrimaryIndex();
			if (!operations.mergeData(
					index,
					adapterStore,
					adapterIndexMappingStore)) {
				JCommander.getConsole().println(
						"Unable to merge data within index '" + index.getId().getString() + "'");
			}
			else {
				JCommander.getConsole().println(
						"Data successfully merged within index '" + index.getId().getString() + "'");
			}
		}

	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName,
			final String adapterId ) {
		parameters = Arrays.asList(
				storeName,
				adapterId);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}
}
