package mil.nga.giat.geowave.core.store.operations.remote;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class RemoteOperationsProvider implements
		CLIOperationProviderSpi
{

	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		RemoteSection.class,
		CalculateStatCommand.class,
		ClearCommand.class,
		ListAdapterCommand.class,
		ListIndexCommand.class,
		ListStatsCommand.class,
		MergeDataCommand.class,
		RecalculateStatsCommand.class,
		RemoveAdapterCommand.class,
		RemoveIndexCommand.class,
		RemoveStatCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
