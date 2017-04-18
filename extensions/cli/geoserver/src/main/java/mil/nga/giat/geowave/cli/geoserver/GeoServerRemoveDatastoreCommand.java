package mil.nga.giat.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rmds", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer DataStore")
public class GeoServerRemoveDatastoreCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "Workspace Name")
	private String workspace;

	@Parameter(description = "<datastore name>")
	private List<String> parameters = new ArrayList<String>();
	private String datastoreName = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		super.prepare(params);
		if (geoserverClient == null) {
			// Create the rest client
			geoserverClient = new GeoServerRestClient(
					new GeoServerConfig(
							getGeoWaveConfigFile(params)));
		}

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <datastore name>");
		}

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		datastoreName = parameters.get(0);

		Response deleteStoreResponse = geoserverClient.deleteDatastore(
				workspace,
				datastoreName);

		if (deleteStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("Delete store '" + datastoreName + "' from workspace '" + workspace
					+ "' on GeoServer: OK");
		}
		else {
			System.err.println("Error deleting store '" + datastoreName + "' from workspace '" + workspace
					+ "' on GeoServer; code = " + deleteStoreResponse.getStatus());
		}
	}
}
