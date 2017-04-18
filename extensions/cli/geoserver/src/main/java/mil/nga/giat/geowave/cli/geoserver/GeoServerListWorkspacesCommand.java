package mil.nga.giat.geowave.cli.geoserver;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listws", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer workspaces")
public class GeoServerListWorkspacesCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		super.prepare(params);
		// Get the local config for GeoServer
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
		Response getWorkspacesResponse = geoserverClient.getWorkspaces();

		if (getWorkspacesResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nList of GeoServer workspaces:");

			JSONObject jsonResponse = JSONObject.fromObject(getWorkspacesResponse.getEntity());

			final JSONArray workspaces = jsonResponse.getJSONArray("workspaces");
			for (int i = 0; i < workspaces.size(); i++) {
				String wsName = workspaces.getJSONObject(
						i).getString(
						"name");
				System.out.println("  > " + wsName);
			}

			System.out.println("---\n");
		}
		else {
			System.err.println("Error getting GeoServer workspace list; code = " + getWorkspacesResponse.getStatus());
		}
	}
}
