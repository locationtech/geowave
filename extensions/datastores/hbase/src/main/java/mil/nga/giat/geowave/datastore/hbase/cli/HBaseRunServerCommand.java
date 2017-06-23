package mil.nga.giat.geowave.datastore.hbase.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "runserver", parentOperation = HBaseSection.class)
@Parameters(commandDescription = "Runs a standalone mini HBase server for test and debug with GeoWave")
public class HBaseRunServerCommand extends
		DefaultOperation implements
		Command
{
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRunServerCommand.class);

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {
		try {
			HBaseMiniCluster.main(new String[] {});
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to run HBase mini cluster",
					e);
		}
	}
}
