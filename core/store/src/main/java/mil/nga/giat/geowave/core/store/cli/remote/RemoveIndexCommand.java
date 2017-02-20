package mil.nga.giat.geowave.core.store.cli.remote;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "rmindex", parentOperation = RemoteSection.class)
@Parameters(hidden = true, commandDescription = "Remove an index from the remote store and all associated data for the index")
public class RemoveIndexCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<store name> <indexId>")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <store name> <indexId>");
		}

		throw new UnsupportedOperationException(
				"This operation is not yet supported");
	}

}
