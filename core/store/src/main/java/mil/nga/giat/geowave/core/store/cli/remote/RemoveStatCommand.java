package mil.nga.giat.geowave.core.store.cli.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;

@GeowaveOperation(name = "rmstat", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Remove a statistic from the remote store. You will be prompted with are you sure")
public class RemoveStatCommand extends
		AbstractStatsCommand implements
		Command
{

	@Parameter(description = "<store name> <adapterId> <statId>")
	private List<String> parameters = new ArrayList<String>();

	private String statId = null;

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

	@Override
	protected boolean calculateStatistics(
			final DataStorePluginOptions storeOptions,
			final DataAdapter<?> adapter,
			final String[] authorizations )
			throws IOException {

		// Remove the stat
		DataStatisticsStore statStore = storeOptions.createDataStatisticsStore();

		if (!statStore.removeStatistics(
				adapter.getAdapterId(),
				new ByteArrayId(
						statId),
				authorizations)) {
			throw new RuntimeException(
					"Unable to remove statistic: " + statId);
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
