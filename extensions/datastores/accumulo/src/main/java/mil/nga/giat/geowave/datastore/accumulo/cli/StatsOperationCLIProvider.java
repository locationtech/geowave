package mil.nga.giat.geowave.datastore.accumulo.cli;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;

public class StatsOperationCLIProvider implements
		CLIOperationProviderSpi
{
	private static final CLIOperationCategory CATEGORY = new StatsOperationCategory();

	@Override
	public CLIOperationCategory getCategory() {
		return CATEGORY;
	}

	@Override
	public CLIOperation[] getOperations() {
		return new CLIOperation[] {
			new CLIOperation(
					"stats",
					"Calculate the statistics of an existing GeoWave dataset",
					new ReCalculateStatsOperation()),
			new CLIOperation(
					"statsdump",
					"Print statistics of an existing GeoWave dataset to standard output",
					new DumpStatsOperation())
		};
	}
}
