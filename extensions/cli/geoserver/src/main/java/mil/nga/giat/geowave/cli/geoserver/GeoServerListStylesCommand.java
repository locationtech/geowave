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

@GeowaveOperation(name = "liststyles", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer styles")
public class GeoServerListStylesCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

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
