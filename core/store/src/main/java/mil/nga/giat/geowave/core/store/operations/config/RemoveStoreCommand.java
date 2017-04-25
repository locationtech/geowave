package mil.nga.giat.geowave.core.store.operations.config;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

import java.io.File;

import org.shaded.restlet.data.Form;
import org.shaded.restlet.representation.Representation;
import org.shaded.restlet.data.Status;
import org.shaded.restlet.resource.Post;
import static mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation.RestEnabledType.*;

@GeowaveOperation(name = "rmstore", parentOperation = ConfigSection.class, restEnabled = POST)
@Parameters(commandDescription = "Remove store from Geowave configuration")
public class RemoveStoreCommand extends
		AbstractRemoveCommand implements
		Command
{

	@Override
	public Void computeResults(
			OperationParams params ) {

		// Search for properties relevant to the given name
		pattern = DataStorePluginOptions.getStoreNamespace(getEntryName());
		return super.computeResults(params);

	}

	@Override
	public void execute(
			OperationParams params ) {
		computeResults(params);
	}
}
