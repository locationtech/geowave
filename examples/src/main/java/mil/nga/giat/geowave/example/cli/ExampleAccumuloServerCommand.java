package mil.nga.giat.geowave.example.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.examples.accumulo.app.GeoWaveDemoApp;

@GeowaveOperation(name = "accumulo-server", parentOperation = ExampleSection.class)
@Parameters(commandDescription = "Runs a standalone mini Accumulo server for test and debug with GeoWave")
public class ExampleAccumuloServerCommand extends
		DefaultOperation implements
		Command
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ExampleHBaseServerCommand.class);

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {
		try {
			GeoWaveDemoApp.main(new String[] {});
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to run Accumulo mini cluster",
					e);
		}
	}
}
