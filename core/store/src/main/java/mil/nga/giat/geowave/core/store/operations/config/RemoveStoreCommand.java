package mil.nga.giat.geowave.core.store.operations.config;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

@GeowaveOperation(name = "rmstore", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Remove store from Geowave configuration")
public class RemoveStoreCommand extends
		AbstractRemoveCommand implements
		Command
{
	@Override
	public void execute(
			OperationParams params ) {

		// Search for properties relevant to the given name
		String pattern = DataStorePluginOptions.getStoreNamespace(getEntryName());
		super.execute(
				params,
				pattern);
	}
}