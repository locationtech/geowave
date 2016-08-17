package mil.nga.giat.geowave.operations.raster;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class RasterOperationCLIProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		RasterSection.class,
		ResizeCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
