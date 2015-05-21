package mil.nga.giat.geowave.datastore.accumulo.util;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;
import mil.nga.giat.geowave.core.cli.CustomOperationCategory;

public class StatsOperationCLIProvider implements
		CLIOperationProviderSpi
{

	@Override
	public CLIOperationCategory getCategory() {
		return new CustomOperationCategory(
				"stats",
				"Statistics",
				"Calculate the statistics of an existing GeoWave dataset");
	}

	@Override
	public CLIOperation[] getOperations() {
		return new CLIOperation[] {
			new CLIOperation(
					"stats",
					"Calculate the statistics of an existing GeoWave dataset",
					new StatsOperation())
		};
	}
}
