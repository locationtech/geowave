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

@GeowaveOperation(name = "addcs", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer coverage store")
public class GeoServerAddCoverageStoreCommand extends
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
		"--coverageStore"
	}, required = false, description = "<coverage store name>")
	private String coverageStore = null;

	@Parameter(names = {
		"-histo",
		"--equalizeHistogramOverride"
	}, required = false, description = "This parameter will override the behavior to always perform histogram equalization if a histogram exists.  Valid values are true and false.", arity = 1)
	private Boolean equalizeHistogramOverride = null;

	@Parameter(names = {
		"-interp",
		"--interpolationOverride"
	}, required = false, description = "This will override the default interpolation stored for each layer.  Valid values are 0, 1, 2, 3 for NearestNeighbor, Bilinear, Bicubic, and Bicubic (polynomial variant) resepctively. ")
	private String interpolationOverride = null;

	@Parameter(names = {
		"-scale",
		"--scaleTo8Bit"
	}, required = false, description = "By default, integer values will automatically be scaled to 8-bit and floating point values will not.  This can be overridden setting this value to true or false.", arity = 1)
	private Boolean scaleTo8Bit = null;

	@Parameter(description = "<GeoWave store name>")
	private List<String> parameters = new ArrayList<String>();
	private String gwStore = null;

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
					"Requires argument: <GeoWave store name>");
		}

		gwStore = parameters.get(0);

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		Response addStoreResponse = geoserverClient.addCoverageStore(
				workspace,
				coverageStore,
				gwStore,
				equalizeHistogramOverride,
				interpolationOverride,
				scaleTo8Bit);

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
