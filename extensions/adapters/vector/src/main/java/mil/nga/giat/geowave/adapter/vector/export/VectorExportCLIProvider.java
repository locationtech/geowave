package mil.nga.giat.geowave.adapter.vector.export;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;

public class VectorExportCLIProvider implements
		CLIOperationProviderSpi
{
	// TODO wire it in with the new command line tools
	@Override
	public CLIOperation[] getOperations() {
		return null;
	}

	@Override
	public CLIOperationCategory getCategory() {
		return null;
	}

}
