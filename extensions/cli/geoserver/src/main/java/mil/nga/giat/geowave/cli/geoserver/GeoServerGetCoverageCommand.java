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

@GeowaveOperation(name = "getcv", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Get a GeoServer coverage's info")
public class GeoServerGetCoverageCommand extends
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
		"-cs",
		"--cvgstore"
	}, required = true, description = "<coverage store name>")
	private String cvgstore = null;

	@Parameter(description = "<coverage name>")
	private List<String> parameters = new ArrayList<String>();
	private String cvgName = null;

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
					"Requires argument: <coverage name>");
		}

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		cvgName = parameters.get(0);

		Response getCvgResponse = geoserverClient.getCoverage(
				workspace,
				cvgstore,
				cvgName);

		if (getCvgResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer coverage info for '" + cvgName + "':");

			JSONObject jsonResponse = JSONObject.fromObject(getCvgResponse.getEntity());
			System.out.println(jsonResponse.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer coverage info for " + cvgName + "; code = "
					+ getCvgResponse.getStatus());
		}
	}
}
