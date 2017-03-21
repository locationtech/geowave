package mil.nga.giat.geowave.core.store.operations.config;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

import org.shaded.restlet.data.Form;
import org.shaded.restlet.representation.Representation;
import org.shaded.restlet.data.Status;
import org.shaded.restlet.resource.Post;

@GeowaveOperation(name = "rmstore", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Remove store from Geowave configuration")
public class RemoveStoreCommand extends
		AbstractRemoveCommand implements
		Command
{

	public void computeResults(
			OperationParams params ) {

		// Search for properties relevant to the given name
		String pattern = DataStorePluginOptions.getStoreNamespace(getEntryName());
		super.computeResults(
				params,
				pattern);

	}

	@Override
	public void execute(
			OperationParams params ) {
		computeResults(params);
	}

	@Post("form:json")
	public void restPost(
			Representation entity ) {
		Form form = new Form(
				entity);
		String name = form.getFirstValue("name");
		// String name = getQueryValue("name");
		if (name == null) {
			this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return;
		}
		setEntryName(name);
		OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				ConfigOptions.getDefaultPropertyFile());
		computeResults(params);
	}
}
