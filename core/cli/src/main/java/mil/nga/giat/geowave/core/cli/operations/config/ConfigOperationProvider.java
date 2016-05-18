package mil.nga.giat.geowave.core.cli.operations.config;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class ConfigOperationProvider implements
		CLIOperationProviderSpi
{

	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		ConfigSection.class,
		ListCommand.class,
		SetCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
