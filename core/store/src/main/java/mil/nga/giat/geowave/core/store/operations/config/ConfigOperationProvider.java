package mil.nga.giat.geowave.core.store.operations.config;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;
import mil.nga.giat.geowave.core.store.operations.config.AddIndexCommand;
import mil.nga.giat.geowave.core.store.operations.config.AddIndexGroupCommand;
import mil.nga.giat.geowave.core.store.operations.config.AddStoreCommand;
import mil.nga.giat.geowave.core.store.operations.config.CopyIndexCommand;
import mil.nga.giat.geowave.core.store.operations.config.CopyStoreCommand;
import mil.nga.giat.geowave.core.store.operations.config.RemoveIndexCommand;
import mil.nga.giat.geowave.core.store.operations.config.RemoveIndexGroupCommand;
import mil.nga.giat.geowave.core.store.operations.config.RemoveStoreCommand;

public class ConfigOperationProvider implements
		CLIOperationProviderSpi
{

	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		AddIndexCommand.class,
		AddIndexGroupCommand.class,
		AddStoreCommand.class,
		CopyIndexCommand.class,
		CopyStoreCommand.class,
		RemoveIndexCommand.class,
		RemoveIndexGroupCommand.class,
		RemoveStoreCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
