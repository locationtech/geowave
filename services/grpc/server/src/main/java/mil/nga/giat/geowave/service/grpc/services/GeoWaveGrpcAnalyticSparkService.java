package mil.nga.giat.geowave.service.grpc.services;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpcshaded.BindableService;
import io.grpcshaded.stub.StreamObserver;
import com.googleshaded.protobuf.Descriptors.FieldDescriptor;

import mil.nga.giat.geowave.analytic.spark.kmeans.operations.KmeansSparkCommand;
import mil.nga.giat.geowave.analytic.spark.sparksql.operations.SparkSqlCommand;
import mil.nga.giat.geowave.analytic.spark.spatial.operations.SpatialJoinCommand;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.service.grpc.protobuf.KmeansSparkCommandParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.SparkSqlCommandParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.SpatialJoinCommandParameters;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import mil.nga.giat.geowave.service.grpc.protobuf.AnalyticSparkGrpc;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;

public class GeoWaveGrpcAnalyticSparkService extends
		AnalyticSparkGrpc.AnalyticSparkImplBase implements
		GeoWaveGrpcServiceSpi
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcAnalyticSparkService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void kmeansSparkCommand(
			KmeansSparkCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		KmeansSparkCommand cmd = new KmeansSparkCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.SetGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);
		LOGGER.info("Executing KmeansSparkCommand...");
		try {
			cmd.computeResults(params);
			VoidResponse resp = VoidResponse.newBuilder().build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void sparkSqlCommand(
			SparkSqlCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		SparkSqlCommand cmd = new SparkSqlCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.SetGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);
		LOGGER.info("Executing SparkSqlCommand...");
		try {
			cmd.computeResults(params);
			VoidResponse resp = VoidResponse.newBuilder().build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void spatialJoinCommand(
			SpatialJoinCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		SpatialJoinCommand cmd = new SpatialJoinCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.SetGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);
		LOGGER.info("Executing SpatialJoinCommand...");
		try {
			cmd.computeResults(params);
			VoidResponse resp = VoidResponse.newBuilder().build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

}
