package mil.nga.giat.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import net.sf.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "addfl", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer feature layer")
public class GeoServerAddFeatureLayerCommand extends
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
	}, required = true, description = "<datastore name>")
	private String datastore = null;

	@Parameter(description = "<layer name>")
	private List<String> parameters = new ArrayList<String>();
	private String layerName = null;

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
					"Requires argument: <layer name>");
		}

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		layerName = parameters.get(0);

		Response addLayerResponse = geoserverClient.addFeatureLayer(
				workspace,
				datastore,
				layerName,
				null);

		if (addLayerResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("\nGeoServer add layer response " + layerName + ":");
			JSONObject listObj = JSONObject.fromObject(addLayerResponse.getEntity());
			System.out.println(listObj.toString(2));
		}
		else {
			System.err
					.println("Error adding GeoServer layer " + layerName + "; code = " + addLayerResponse.getStatus());
		}
	}
}
