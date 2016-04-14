package mil.nga.giat.geowave.adapter.raster.operations;

import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;

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
