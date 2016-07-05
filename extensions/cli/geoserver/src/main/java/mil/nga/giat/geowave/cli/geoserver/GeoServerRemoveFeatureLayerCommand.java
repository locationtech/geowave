package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import net.sf.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rmfl", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer feature Layer")
public class GeoServerRemoveFeatureLayerCommand implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(description = "<layer name>")
	private List<String> parameters = new ArrayList<String>();
	private String layerName = null;

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
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <layer name>");
		}

		layerName = parameters.get(0);

		Response deleteLayerResponse = geoserverClient.deleteFeatureLayer(layerName);

		if (deleteLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer delete layer response " + layerName + ":");
			JSONObject listObj = JSONObject.fromObject(deleteLayerResponse.getEntity());
			System.out.println(listObj.toString(2));
		}
		else {
			System.err.println("Error deleting GeoServer layer " + layerName + "; code = " + deleteLayerResponse.getStatus());
		}
	}
}
