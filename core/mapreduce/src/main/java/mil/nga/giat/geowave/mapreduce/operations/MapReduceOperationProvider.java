package mil.nga.giat.geowave.mapreduce.operations;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class MapReduceOperationProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		CopyCommand.class,
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
