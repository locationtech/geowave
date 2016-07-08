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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "addcs", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer coverage store")
public class GeoServerAddCoverageStoreCommand implements
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
		"--coverageStore"
	}, required = false, description = "<coverage store name>")
	private String coverageStore = null;

	@Parameter(description = "<GeoWave store name>")
	private List<String> parameters = new ArrayList<String>();
	private String gwStore = null;

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
					"Requires argument: <GeoWave store name>");
		}

		gwStore = parameters.get(0);

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		Response addStoreResponse = geoserverClient.addCoverageStore(
				workspace,
				coverageStore,
				gwStore);

		if (addStoreResponse.getStatus() == Status.OK.getStatusCode()
				|| addStoreResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("Add coverage store for '" + gwStore + "' to workspace '" + workspace
					+ "' on GeoServer: OK");
		}
		else {
			System.err.println("Error adding coverage store for '" + gwStore + "' to workspace '" + workspace
					+ "' on GeoServer; code = " + addStoreResponse.getStatus());
		}
	}
}
