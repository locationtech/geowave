package mil.nga.giat.geowave.service.grpc.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServer;

@GeowaveOperation(name = "stop", parentOperation = GrpcSection.class)
@Parameters(commandDescription = "terminates the GeoWave grpc server")
public class StopGrpcServerCommand extends
		DefaultOperation implements
		Command
{
	private static final Logger LOGGER = LoggerFactory.getLogger(StartGrpcServerCommand.class);

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {

		LOGGER.info("Stopping GeoWave grpc server");
		GeoWaveGrpcServer.getInstance().stop();
	}
}
