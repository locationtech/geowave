package mil.nga.giat.geowave.format.landsat8;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "ingestvector", parentOperation = Landsat8Section.class)
@Parameters(commandDescription = "Ingest routine for searching landsat scenes that match certain criteria and ingesting the scene and band metadata into GeoWave's vector store.")
public class Landsat8IngestVectorCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<storename> <comma delimited index/group list>")
	private final List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	protected Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();

	public Landsat8IngestVectorCommand() {}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		final VectorIngestRunner runner = new VectorIngestRunner(
				analyzeOptions,
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
