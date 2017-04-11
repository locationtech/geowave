package mil.nga.giat.geowave.core.cli.operations.config.security;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class SecurityOperationProvider implements
		CLIOperationProviderSpi
{

	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		SecuritySection.class,
		NewTokenCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}