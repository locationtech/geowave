package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "liststyles", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer styles")
public class GeoServerListStylesCommand implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		if (geoserverClient == null) {
			// Get the local config for GeoServer
			File propFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);

			GeoServerConfig config = new GeoServerConfig(
					propFile);

			// Create the rest client
			geoserverClient = new GeoServerRestClient(
					config);
		}

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {

		Response listStylesResponse = geoserverClient.getStyles();

		if (listStylesResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer styles list:");

			JSONObject jsonResponse = JSONObject.fromObject(listStylesResponse.getEntity());
			JSONArray styles = jsonResponse.getJSONArray("styles");
			System.out.println(styles.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer styles list; code = " + listStylesResponse.getStatus());
		}
	}
}
