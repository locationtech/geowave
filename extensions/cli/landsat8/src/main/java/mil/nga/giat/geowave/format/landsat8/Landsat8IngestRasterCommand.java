package mil.nga.giat.geowave.format.landsat8;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import it.geosolutions.jaiext.JAIExt;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "ingestraster", parentOperation = Landsat8Section.class)
@Parameters(commandDescription = "Ingest routine for locally downloading Landsat 8 imagery and ingesting it into GeoWave")
public class Landsat8IngestRasterCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<storename> <comma delimited index/group list>")
	private final List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	protected Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();

	@ParametersDelegate
	protected Landsat8DownloadCommandLineOptions downloadOptions = new Landsat8DownloadCommandLineOptions();

	@ParametersDelegate
	protected Landsat8RasterIngestCommandLineOptions ingestOptions = new Landsat8RasterIngestCommandLineOptions();

	public Landsat8IngestRasterCommand() {}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		JAIExt.initJAIEXT();
		final RasterIngestRunner runner = new RasterIngestRunner(
				analyzeOptions,
				downloadOptions,
				ingestOptions,
				parameters);
		runner.runInternal(params);
	}

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}

}
