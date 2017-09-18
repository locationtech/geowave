package mil.nga.giat.geowave.datastore.accumulo.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.datastore.accumulo.app.AccumuloMiniCluster;

@GeowaveOperation(name = "runserver", parentOperation = AccumuloSection.class)
@Parameters(commandDescription = "Runs a standalone mini Accumulo server for test and debug with GeoWave")
public class AccumuloRunServerCommand extends
		DefaultOperation implements
		Command
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloRunServerCommand.class);

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {
		try {
			AccumuloMiniCluster.main(new String[] {});
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to run Accumulo mini cluster",
					e);
		}
	}
}
