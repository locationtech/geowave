package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "getstyle", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Get GeoServer Style info")
public class GeoServerGetStyleCommand implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(description = "<style name>")
	private List<String> parameters = new ArrayList<String>();
	private String style = null;

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
					"Requires argument: <style name>");
		}

		style = parameters.get(0);

		Response getStyleResponse = geoserverClient.getStyle(style);

		if (getStyleResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer style info for '" + style + "':");

			final String style = IOUtils.toString((InputStream) getStyleResponse.getEntity());
			System.out.println(style);

		}
		else {
			System.err.println("Error getting GeoServer style info for '" + style + "'; code = "
					+ getStyleResponse.getStatus());
		}
	}
}
