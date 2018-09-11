package mil.nga.giat.geowave.service.grpc.cli;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServer;

@GeowaveOperation(name = "start", parentOperation = GrpcSection.class)
@Parameters(commandDescription = "Runs a gRPC service for GeoWave commands")
public class StartGrpcServerCommand extends
		DefaultOperation implements
		Command
{
	private static final Logger LOGGER = LoggerFactory.getLogger(StartGrpcServerCommand.class);

	@ParametersDelegate
	private StartGrpcServerCommandOptions options = new StartGrpcServerCommandOptions();

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {

		LOGGER.info("Starting GeoWave grpc server on port: " + options.getPort());
		GeoWaveGrpcServer server = null;

		server = GeoWaveGrpcServer.getInstance();

		try {
			server.start(options.getPort());
		}
		catch (final IOException | NullPointerException e) {
			LOGGER.error(
					"Exception encountered starting gRPC server",
					e);
		}

		if (!options.isNonBlocking()) {
			try {
				server.blockUntilShutdown();
			}
			catch (InterruptedException e) {
				LOGGER.error(
						"Exception encountered during gRPC server blockUntilShutdown()",
						e);
			}
		}

		LOGGER.info("GeoWave grpc server started successfully");
	}

	public void setCommandOptions(
			final StartGrpcServerCommandOptions opts ) {
		options = opts;
	}
}