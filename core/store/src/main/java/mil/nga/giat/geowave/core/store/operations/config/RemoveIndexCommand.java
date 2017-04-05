package mil.nga.giat.geowave.core.store.operations.config;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

@GeowaveOperation(name = "rmindex", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Remove index configuration from Geowave configuration")
public class RemoveIndexCommand extends
		AbstractRemoveCommand implements
		Command
{

	@Override
	public void execute(
			OperationParams params ) {

		// Search for properties relevant to the given name
		String pattern = IndexPluginOptions.getIndexNamespace(getEntryName());
		super.execute(
				params,
				pattern);

	}
}