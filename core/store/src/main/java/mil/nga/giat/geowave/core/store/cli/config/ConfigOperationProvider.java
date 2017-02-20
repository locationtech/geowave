package mil.nga.giat.geowave.core.store.cli.config;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;
import mil.nga.giat.geowave.core.store.cli.config.AddIndexCommand;
import mil.nga.giat.geowave.core.store.cli.config.AddIndexGroupCommand;
import mil.nga.giat.geowave.core.store.cli.config.AddStoreCommand;
import mil.nga.giat.geowave.core.store.cli.config.CopyIndexCommand;
import mil.nga.giat.geowave.core.store.cli.config.CopyStoreCommand;
import mil.nga.giat.geowave.core.store.cli.config.RemoveIndexCommand;
import mil.nga.giat.geowave.core.store.cli.config.RemoveIndexGroupCommand;
import mil.nga.giat.geowave.core.store.cli.config.RemoveStoreCommand;

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
