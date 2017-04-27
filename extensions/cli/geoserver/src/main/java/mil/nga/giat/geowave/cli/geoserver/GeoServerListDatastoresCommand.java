package mil.nga.giat.geowave.cli.geoserver;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listds", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer datastores")
public class GeoServerListDatastoresCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "<workspace name>")
	private String workspace;

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
		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		Response listStoresResponse = geoserverClient.getDatastores(workspace);

		if (listStoresResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer stores list for '" + workspace + "':");

			JSONObject jsonResponse = JSONObject.fromObject(listStoresResponse.getEntity());
			JSONArray datastores = jsonResponse.getJSONArray("dataStores");
			System.out.println(datastores.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer stores list for '" + workspace + "'; code = "
					+ listStoresResponse.getStatus());
		}
	}
}
