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

@GeowaveOperation(name = "addds", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer datastore")
public class GeoServerAddDatastoreCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "<workspace name>")
	private String workspace = null;

	@Parameter(names = {
		"-ds",
		"--datastore"
	}, required = false, description = "<datastore name>")
	private String datastore = null;

	@Parameter(description = "<GeoWave store name>")
	private List<String> parameters = new ArrayList<String>();
	private String gwStore = null;

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

		gwStore = parameters.get(0);

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		Response addStoreResponse = geoserverClient.addDatastore(
				workspace,
				datastore,
				gwStore);

		if (addStoreResponse.getStatus() == Status.OK.getStatusCode()
				|| addStoreResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("Add datastore for '" + gwStore + "' to workspace '" + workspace + "' on GeoServer: OK");
		}
		else {
			System.err.println("Error adding datastore for '" + gwStore + "' to workspace '" + workspace
					+ "' on GeoServer; code = " + addStoreResponse.getStatus());
		}
	}
}
