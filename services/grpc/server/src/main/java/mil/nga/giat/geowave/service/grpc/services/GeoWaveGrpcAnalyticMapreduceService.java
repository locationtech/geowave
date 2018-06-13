package mil.nga.giat.geowave.service.grpc.services;

import java.io.File;
import java.util.Map;

import io.grpcshaded.BindableService;
import io.grpcshaded.stub.StreamObserver;
import com.googleshaded.protobuf.Descriptors.FieldDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.mapreduce.operations.DBScanCommand;
import mil.nga.giat.geowave.analytic.mapreduce.operations.KdeCommand;
import mil.nga.giat.geowave.analytic.mapreduce.operations.NearestNeighborCommand;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import mil.nga.giat.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc;
import mil.nga.giat.geowave.service.grpc.protobuf.DBScanCommandParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;
import mil.nga.giat.geowave.service.grpc.protobuf.KdeCommandParameters;
import mil.nga.giat.geowave.service.grpc.protobuf.NearestNeighborCommandParameters;

public class GeoWaveGrpcAnalyticMapreduceService extends
		AnalyticMapreduceGrpc.AnalyticMapreduceImplBase implements
		GeoWaveGrpcServiceSpi
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcAnalyticMapreduceService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void kdeCommand(
			KdeCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		KdeCommand cmd = new KdeCommand();
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
		try {
			cmd.computeResults(params);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
		LOGGER.info("Executing KdeCommand...");
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
	public void dBScanCommand(
			DBScanCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		DBScanCommand cmd = new DBScanCommand();
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
		LOGGER.info("Executing DBScanCommand...");
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
	public void nearestNeighborCommand(
			NearestNeighborCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		NearestNeighborCommand cmd = new NearestNeighborCommand();
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
		LOGGER.info("Executing NearestNeighborCommand...");
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
