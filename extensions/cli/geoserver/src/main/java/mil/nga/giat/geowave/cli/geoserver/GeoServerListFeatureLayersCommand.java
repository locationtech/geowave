package mil.nga.giat.geowave.cli.geoserver;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

import net.sf.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listfl", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer feature layers")
public class GeoServerListFeatureLayersCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "Workspace Name")
	private String workspace = null;

	@Parameter(names = {
		"-ds",
		"--datastore"
	}, required = false, description = "Datastore Name")
	private String datastore = null;

	@Parameter(names = {
		"-g",
		"--geowaveOnly"
	}, required = false, description = "Show only GeoWave feature layers (default: false)")
	private Boolean geowaveOnly = false;

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
		Response listLayersResponse = geoserverClient.getFeatureLayers(
				workspace,
				datastore,
				geowaveOnly);

		if (listLayersResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer layer list:");
			JSONObject listObj = JSONObject.fromObject(listLayersResponse.getEntity());
			System.out.println(listObj.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer layer list; code = " + listLayersResponse.getStatus());
		}
	}
}
