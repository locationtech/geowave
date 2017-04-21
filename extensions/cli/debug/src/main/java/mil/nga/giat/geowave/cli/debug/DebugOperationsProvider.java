package mil.nga.giat.geowave.cli.debug;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class DebugOperationsProvider implements
		CLIOperationProviderSpi
{

	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		DebugSection.class,
		BBOXQuery.class,
		ClientSideCQLQuery.class,
		CQLQuery.class,
		FullTableScan.class,
		MinimalFullTable.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
