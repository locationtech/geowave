package mil.nga.giat.geowave.test.service.grpc;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.netty.NettyServerBuilder;
import io.grpc.Server;

import mil.nga.giat.geowave.service.grpc.services.GeoWaveGrpcAnalyticMapreduceService;
import mil.nga.giat.geowave.service.grpc.services.GeoWaveGrpcAnalyticSparkService;
import mil.nga.giat.geowave.service.grpc.services.GeoWaveGrpcCliGeoserverService;
import mil.nga.giat.geowave.service.grpc.services.GeoWaveGrpcCoreCliService;
import mil.nga.giat.geowave.service.grpc.services.GeoWaveGrpcCoreIngestService;
import mil.nga.giat.geowave.service.grpc.services.GeoWaveGrpcCoreMapreduceService;
import mil.nga.giat.geowave.service.grpc.services.GeoWaveGrpcCoreStoreService;
import mil.nga.giat.geowave.service.grpc.services.GeoWaveGrpcVectorService;

public class GeoWaveGrpcTestServer
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcTestServer.class.getName());

	private final int port;
	private final Server server;

	public GeoWaveGrpcTestServer(
			int port )
			throws IOException {
		this.port = port;

		// Add all services here

		server = NettyServerBuilder.forPort(
				port).addService(
				new GeoWaveGrpcVectorService()).addService(
				new GeoWaveGrpcAnalyticMapreduceService()).addService(
				new GeoWaveGrpcAnalyticSparkService()).addService(
				new GeoWaveGrpcCliGeoserverService()).addService(
				new GeoWaveGrpcCoreCliService()).addService(
				new GeoWaveGrpcCoreIngestService()).addService(
				new GeoWaveGrpcCoreMapreduceService()).addService(
				new GeoWaveGrpcCoreStoreService()).build();
		LOGGER.warn("Server made " + server.toString());
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
						GeoWaveGrpcTestServer.this.stop();
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
