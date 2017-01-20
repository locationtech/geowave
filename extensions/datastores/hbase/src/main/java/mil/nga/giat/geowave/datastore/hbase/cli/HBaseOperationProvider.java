package mil.nga.giat.geowave.datastore.hbase.cli;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class HBaseOperationProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		HBaseSection.class,
		CombineStatisticsCommand.class,
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
