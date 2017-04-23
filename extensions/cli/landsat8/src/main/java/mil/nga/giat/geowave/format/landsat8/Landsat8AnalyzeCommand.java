package mil.nga.giat.geowave.format.landsat8;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "analyze", parentOperation = Landsat8Section.class)
@Parameters(commandDescription = "Print out basic aggregate statistics for available Landsat 8 imagery")
public class Landsat8AnalyzeCommand extends
		DefaultOperation implements
		Command
{
	@ParametersDelegate
	protected Landsat8BasicCommandLineOptions landsatOptions = new Landsat8BasicCommandLineOptions();

	public Landsat8AnalyzeCommand() {}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		final AnalyzeRunner runner = new AnalyzeRunner(
				landsatOptions);
		runner.runInternal(params);
	}

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}

}
