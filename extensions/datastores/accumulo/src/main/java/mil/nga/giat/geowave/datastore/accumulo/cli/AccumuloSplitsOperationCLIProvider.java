package mil.nga.giat.geowave.datastore.accumulo.cli;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class AccumuloSplitsOperationCLIProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		AccumuloSection.class,
		PreSplitPartitionIdCommand.class,
		SplitEqualIntervalCommand.class,
		SplitNumRecordsCommand.class,
		SplitQuantileCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
