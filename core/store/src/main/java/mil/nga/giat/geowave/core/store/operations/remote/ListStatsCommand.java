package mil.nga.giat.geowave.core.store.operations.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StatsCommandLineOptions;

@GeowaveOperation(name = "liststats", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Print statistics of an existing GeoWave dataset to standard output.  ")
public class ListStatsCommand extends
		AbstractStatsCommand implements
		Command
{

	private static final Logger LOGGER = LoggerFactory.getLogger(ListStatsCommand.class);

	@Parameter(description = "<store name> [<adapter name>]")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params )
			throws ParameterException {

		// Ensure we have all the required arguments
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Requires arguments: <store name> [<adapterId>]");
		}

		super.run(
				params,
				parameters);
	}

	@Override
	protected boolean performStatsCommand(
			final DataStorePluginOptions storeOptions,
			final DataAdapter<?> adapter,
			final StatsCommandLineOptions statsOptions )
			throws IOException {

		if (adapter == null) {
			throw new IOException(
					"Provided adapter is null");
		}

		DataStatisticsStore statsStore = storeOptions.createDataStatisticsStore();
		final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

		StringBuilder builder = new StringBuilder();

		try (CloseableIterator<DataStatistics<?>> statsIt = statsStore.getAllDataStatistics(authorizations)) {
			if (statsOptions.getJsonFormatFlag()) {
				JSONArray resultsArray = new JSONArray();
				JSONObject outputObject = new JSONObject();

				try {
					// Output as JSON formatted strings
					outputObject.put(
							"adapter",
							adapter.getAdapterId().getString());
					while (statsIt.hasNext()) {
						final DataStatistics<?> stats = statsIt.next();
						if (!stats.getDataAdapterId().equals(
								adapter.getAdapterId())) {
							continue;
						}
						resultsArray.add(stats.toJSONObject());
					}
					outputObject.put(
							"stats",
							resultsArray);
					builder.append(outputObject.toString());
				}
				catch (final JSONException ex) {
					LOGGER.error(
							"Unable to output statistic as JSON.  ",
							ex);
				}
			}
			// Output as strings
			else {
				while (statsIt.hasNext()) {
					final DataStatistics<?> stats = statsIt.next();
					if (!stats.getDataAdapterId().equals(
							adapter.getAdapterId())) {
						continue;
					}
					builder.append("[");
					builder.append(String.format(
							"%1$-20s",
							stats.getStatisticsId().getString()));
					builder.append("] ");
					builder.append(stats.toString());
					builder.append("\n");
				}
			}
			JCommander.getConsole().println(
					builder.toString().trim());

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
