package mil.nga.giat.geowave.adapter.vector.export;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class VectorExportCLIProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		VectorSection.class,
		VectorLocalExportCommand.class,
		VectorMRExportCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
