package mil.nga.giat.geowave.core.store.operations.config;

import com.beust.jcommander.Parameters;

import com.beust.jcommander.Parameters;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import org.shaded.restlet.data.Status;
import org.shaded.restlet.resource.Delete;
import org.shaded.restlet.resource.Post;
import java.io.File;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import static mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation.RestEnabledType.*;

@GeowaveOperation(name = "rmindex", parentOperation = ConfigSection.class, restEnabled = POST)
@Parameters(commandDescription = "Remove index configuration from Geowave configuration")
public class RemoveIndexCommand extends
		AbstractRemoveCommand implements
		Command
{

	@Override
	public void execute(
			OperationParams params ) {
		computeResults(params);

	}

	protected Void computeResults(
			OperationParams params ) {

		pattern = IndexPluginOptions.getIndexNamespace(getEntryName());
		return super.computeResults(params);
	}

	// TODO i dont' know what to do with this
	@Post("json")
	public void restDelete() {
		String pattern = getQueryValue("pattern");
		if (pattern == null) {
			this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return;
		}
		this.setEntryName(pattern);
		OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				ConfigOptions.getDefaultPropertyFile());
		computeResults(params);
	}

}
