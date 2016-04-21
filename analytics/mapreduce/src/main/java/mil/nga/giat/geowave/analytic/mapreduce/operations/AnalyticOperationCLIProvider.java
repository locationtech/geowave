package mil.nga.giat.geowave.analytic.mapreduce.operations;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class AnalyticOperationCLIProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		AnalyticSection.class,
		DBScanCommand.class,
		KdeCommand.class,
		KmeansJumpCommand.class,
		KmeansParallelCommand.class,
		NearestNeighborCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
