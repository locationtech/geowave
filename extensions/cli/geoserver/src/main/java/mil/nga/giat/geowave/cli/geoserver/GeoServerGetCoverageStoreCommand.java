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

@GeowaveOperation(name = "getcs", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Get GeoServer CoverageStore info")
public class GeoServerGetCoverageStoreCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "<workspace name>")
	private String workspace;

	@Parameter(description = "<coverage store name>")
	private List<String> parameters = new ArrayList<String>();
	private String csName = null;

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

		csName = parameters.get(0);

		Response getCvgStoreResponse = geoserverClient.getCoverageStore(
				workspace,
				csName);

		if (getCvgStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer coverage store info for '" + csName + "':");

			JSONObject jsonResponse = JSONObject.fromObject(getCvgStoreResponse.getEntity());
			JSONObject cvgstore = jsonResponse.getJSONObject("coverageStore");
			System.out.println(cvgstore.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer coverage store info for '" + csName + "'; code = "
					+ getCvgStoreResponse.getStatus());
		}
	}
}
