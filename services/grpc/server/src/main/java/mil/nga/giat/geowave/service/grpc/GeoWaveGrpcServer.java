package mil.nga.giat.geowave.service.grpc;

import java.io.IOException;

import java.util.ServiceLoader;
import java.util.ServiceConfigurationError;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

public class GeoWaveGrpcServer
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcServer.class.getName());
	private Server server = null;

	private static GeoWaveGrpcServer instance;
	private ServiceLoader<GeoWaveGrpcServiceSpi> serviceLoader;

	private GeoWaveGrpcServer() {
		serviceLoader = ServiceLoader.load(GeoWaveGrpcServiceSpi.class);
	}

	public static synchronized GeoWaveGrpcServer getInstance() {
		if (instance == null) {
			instance = new GeoWaveGrpcServer();
		}
		return instance;
	}

	/** Start serving requests. */
	public void start(
			int port )
			throws IOException {
		final ServerBuilder<?> builder = NettyServerBuilder.forPort(port);
		builder.addService(ProtoReflectionService.newInstance());
		try {
			Iterator<GeoWaveGrpcServiceSpi> grpcServices = serviceLoader.iterator();
			while (grpcServices.hasNext()) {
				GeoWaveGrpcServiceSpi s = grpcServices.next();
				builder.addService(s.getBindableService());
			}
		}
		catch (final ServiceConfigurationError e) {
			LOGGER.error(
					"Exception encountered initializing services for gRPC server",
					e);
		}

		server = builder.build();
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
	public void blockUntilShutdown()
			throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
		}
	}

}
