package mil.nga.giat.geowave.adapter.vector.cli;

import mil.nga.giat.geowave.adapter.vector.delete.CQLDelete;
import mil.nga.giat.geowave.adapter.vector.export.VectorLocalExportCommand;
import mil.nga.giat.geowave.adapter.vector.export.VectorMRExportCommand;
import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class VectorCLIProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		VectorSection.class,
		VectorLocalExportCommand.class,
		VectorMRExportCommand.class,
		CQLDelete.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
