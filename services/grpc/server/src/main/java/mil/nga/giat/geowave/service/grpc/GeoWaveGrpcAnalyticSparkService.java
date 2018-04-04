package mil.nga.giat.geowave.service.grpc;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.spark.kmeans.operations.KmeansSparkCommand;
import mil.nga.giat.geowave.analytic.spark.sparksql.operations.SparkSqlCommand;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.service.grpc.protobuf.KmeansSparkCommandParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.SparkSqlCommandParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.AnalyticSparkGrpc;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;

public class GeoWaveGrpcAnalyticSparkService extends
		AnalyticSparkGrpc.AnalyticSparkImplBase
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcAnalyticSparkService.class.getName());

	@Override
	public void kmeansSparkCommand(
			KmeansSparkCommandParameters request,
			io.grpc.stub.StreamObserver<VoidResponse> responseObserver ) {
		KmeansSparkCommand cmd = new KmeansSparkCommand();
		Map<com.google.protoshadebuf3.Descriptors.FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.SetGrpcToCommandFields(
				m,
				cmd);

		final File configFile = ConfigOptions.getDefaultPropertyFile();
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);
		LOGGER.info("Executing KmeansSparkCommand...");
		try {
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
			io.grpc.stub.StreamObserver<VoidResponse> responseObserver ) {
		SparkSqlCommand cmd = new SparkSqlCommand();
		Map<com.google.protoshadebuf3.Descriptors.FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.SetGrpcToCommandFields(
				m,
				cmd);

		final File configFile = ConfigOptions.getDefaultPropertyFile();
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);
		LOGGER.info("Executing SparkSqlCommand...");
		try {
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
