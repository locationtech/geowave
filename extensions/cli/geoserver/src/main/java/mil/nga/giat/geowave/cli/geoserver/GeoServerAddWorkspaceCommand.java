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

@GeowaveOperation(name = "addws", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add GeoServer workspace")
public class GeoServerAddWorkspaceCommand implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(description = "<workspace name>")
	private List<String> parameters = new ArrayList<String>();
	private String wsName = null;

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
					"Requires argument: <workspace name>");
		}

		wsName = parameters.get(0);

		Response addWorkspaceResponse = geoserverClient.addWorkspace(wsName);
		if (addWorkspaceResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("Add workspace '" + wsName + "' to GeoServer: OK");
		}
		else {
			System.err.println("Error adding workspace '" + wsName + "' to GeoServer; code = " + addWorkspaceResponse.getStatus());
		}
	}
}
