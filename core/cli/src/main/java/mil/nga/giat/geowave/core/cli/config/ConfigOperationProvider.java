package mil.nga.giat.geowave.core.cli.config;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;
import mil.nga.giat.geowave.core.cli.CommandObject;
import mil.nga.giat.geowave.core.cli.config.index.AddIndexOperation;

public class ConfigOperationProvider implements
		CLIOperationProviderSpi
{

	@Override
	public CLIOperation[] createOperations() {
		return new CLIOperation[] {
			new AddIndexOperation()
		};
	}

	@Override
	public CommandObject getOperationCategory() {
		return new ConfigOperationCategory();
	}

}
