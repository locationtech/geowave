package mil.nga.giat.geowave.core.store.operations.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreStatisticsProvider;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StatsCommandLineOptions;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

@GeowaveOperation(name = "recalcstats", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Calculate the statistics of an existing GeoWave dataset")
public class RecalculateStatsCommand extends
		AbstractStatsCommand implements
		Command
{

	private static final Logger LOGGER = LoggerFactory.getLogger(RecalculateStatsCommand.class);

	@Parameter(description = "<store name> [<adapter name>]")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		// Ensure we have all the required arguments
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Requires arguments: <store name> [<adapterId>]");
		}

		super.run(
				params,
				parameters);
	}

	protected boolean performStatsCommand(
			final DataStorePluginOptions storeOptions,
			final DataAdapter<?> adapter,
			final StatsCommandLineOptions statsOptions )
			throws IOException {

		try {

			AdapterIndexMappingStore mappingStore = storeOptions.createAdapterIndexMappingStore();
			DataStore dataStore = storeOptions.createDataStore();
			IndexStore indexStore = storeOptions.createIndexStore();

			boolean isFirstTime = true;
			for (final PrimaryIndex index : mappingStore.getIndicesForAdapter(
					adapter.getAdapterId()).getIndices(
					indexStore)) {

				@SuppressWarnings({
					"rawtypes",
					"unchecked"
				})
				DataStoreStatisticsProvider provider = new DataStoreStatisticsProvider(
						adapter,
						index,
						isFirstTime);
				final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

				try (StatsCompositionTool<?> statsTool = new StatsCompositionTool(
						provider,
						storeOptions.createDataStatisticsStore())) {
					try (CloseableIterator<?> entryIt = dataStore.query(
							new QueryOptions(
									adapter,
									index,
									(Integer) null,
									statsTool,
									authorizations),
							(Query) null)) {
						while (entryIt.hasNext()) {
							entryIt.next();
						}
					}
				}
				isFirstTime = false;
			}

		}
		catch (final Exception ex) {
			LOGGER.error(
					"Error while writing statistics.",
					ex);
			return false;
		}

		return true;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName,
			String adapterName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
		if (adapterName != null) {
			this.parameters.add(adapterName);
		}
	}
}
