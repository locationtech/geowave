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
import mil.nga.giat.geowave.core.index.ByteArrayId;
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

@GeowaveOperation(name = "calcstat", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Calculate a specific statistic in the remote store, given adapter ID and statistic ID")
/**
 * This class calculates the statistic(s) in the given store and replaces the
 * existing value.
 */
public class CalculateStatCommand extends
		AbstractStatsCommand implements
		Command
{

	private static final Logger LOGGER = LoggerFactory.getLogger(CalculateStatCommand.class);

	@Parameter(description = "<store name> <adapterId> <statId>")
	private List<String> parameters = new ArrayList<String>();

	// The state we're re-caculating. Set in execute(), used in
	// calculateStatistics()
	private String statId;

	@Override
	public void execute(
			OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <store name> <adapterId> <statId>");
		}

		statId = parameters.get(2);

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

				final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());
				@SuppressWarnings({
					"rawtypes",
					"unchecked"
				})
				DataStoreStatisticsProvider provider = new DataStoreStatisticsProvider(
						adapter,
						index,
						isFirstTime) {
					@Override
					public ByteArrayId[] getSupportedStatisticsTypes() {
						return new ByteArrayId[] {
							new ByteArrayId(
									statId)
						};
					}
				};

				try (@SuppressWarnings({
					"rawtypes",
					"unchecked"
				})
				StatsCompositionTool<?> statsTool = new StatsCompositionTool(
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
			String adapterId,
			String statId ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
		this.parameters.add(adapterId);
		this.parameters.add(statId);
	}
}
