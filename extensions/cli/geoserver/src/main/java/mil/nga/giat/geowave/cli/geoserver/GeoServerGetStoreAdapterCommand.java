package mil.nga.giat.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "getsa", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Get GeoWave store adapters")
public class GeoServerGetStoreAdapterCommand extends
		DefaultOperation implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();
	private String storeName = null;

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
					"Requires argument: <store name>");
		}

		storeName = parameters.get(0);
		ArrayList<String> adapterList = geoserverClient.getStoreAdapters(
				storeName,
				null);

		System.out.println("Store " + storeName + " has these adapters:");
		for (String adapterId : adapterList) {
			System.out.println(adapterId);
		}
	}
}
