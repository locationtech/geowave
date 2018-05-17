package mil.nga.giat.geowave.service.grpc;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpcshaded.Server;
import io.grpcshaded.ServerBuilder;

public class GeoWaveGrpcServer
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcServer.class.getName());

	private final int port;
	private final Server server;

	public static void main(
			String[] args )
			throws InterruptedException {

		LOGGER.info("Starting gRPC server");
		GeoWaveGrpcServer server = null;
		// use default port unless there is a command line argument
		int port = GeoWaveGrpcServiceOptions.port;
		if (args.length > 0) {
			try {
				port = Integer.parseInt(args[0]);
			}
			catch (NumberFormatException e) {
				LOGGER.error(
						"Exception encountered: Argument" + args[0] + " must be an integer.",
						e);
				return;
			}
		}

		try {
			server = new GeoWaveGrpcServer(
					port);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Exception encountered instantiating gRPC server",
					e);
		}

		try {
			server.start();
			// HP Fortify "NULL Pointer Dereference" false positive
			// NullPointerExceptions are being caught
			server.blockUntilShutdown();
		}
		catch (final IOException | NullPointerException e) {
			LOGGER.error(
					"Exception encountered starting gRPC server",
					e);
		}
	}

	public GeoWaveGrpcServer(
			int port )
			throws IOException {
		this.port = port;

		// This is a bare-bones implementation to be used as a template, add
		// more services as desired
		server = ServerBuilder.forPort(
				port).addService(
				new GeoWaveGrpcVectorService()).build();
	}

	/** Start serving requests. */
	public void start()
			throws IOException {
		server.start();
		LOGGER.info("Server started, listening on " + port);
		Runtime.getRuntime().addShutdownHook(
				new Thread() {
					@Override
					public void run() {
						// Use stderr here since the logger may have been reset
						// by its JVM shutdown hook.
						System.err.println("*** shutting down gRPC server since JVM is shutting down");
						GeoWaveGrpcServer.this.stop();
						System.err.println("*** server shut down");
					}
				});
	}

	/** Stop serving requests and shutdown resources. */
	public void stop() {
		if (server != null) {
			server.shutdown();
		}
	}

	/**
	 * Await termination on the main thread since the grpc library uses daemon
	 * threads.
	 */
	private void blockUntilShutdown()
			throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
		}
	}

}
