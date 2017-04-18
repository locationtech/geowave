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

@GeowaveOperation(name = "rmcs", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer Coverage Store")
public class GeoServerRemoveCoverageStoreCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "Workspace Name")
	private String workspace;

	@Parameter(description = "<coverage store name>")
	private List<String> parameters = new ArrayList<String>();
	private String cvgstoreName = null;

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
					"Requires argument: <coverage store name>");
		}

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		cvgstoreName = parameters.get(0);

		Response deleteCvgStoreResponse = geoserverClient.deleteCoverageStore(
				workspace,
				cvgstoreName);

		if (deleteCvgStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("Delete store '" + cvgstoreName + "' from workspace '" + workspace
					+ "' on GeoServer: OK");
		}
		else {
			System.err.println("Error deleting store '" + cvgstoreName + "' from workspace '" + workspace
					+ "' on GeoServer; code = " + deleteCvgStoreResponse.getStatus());
		}
	}
}
