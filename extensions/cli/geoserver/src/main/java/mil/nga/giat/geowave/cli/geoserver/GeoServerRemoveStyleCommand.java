package mil.nga.giat.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rmstyle", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer Style")
public class GeoServerRemoveStyleCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(description = "<style name>")
	private List<String> parameters = new ArrayList<String>();
	private String styleName = null;

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
					"Requires argument: <style name>");
		}

		styleName = parameters.get(0);

		Response deleteStyleResponse = geoserverClient.deleteStyle(styleName);

		if (deleteStyleResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("Delete style '" + styleName + "' on GeoServer: OK");
		}
		else {
			System.err.println("Error deleting style '" + styleName + "' on GeoServer; code = "
					+ deleteStyleResponse.getStatus());
		}
	}
}
