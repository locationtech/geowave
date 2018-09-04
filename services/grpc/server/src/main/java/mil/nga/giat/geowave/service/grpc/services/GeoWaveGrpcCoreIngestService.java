package mil.nga.giat.geowave.service.grpc.services;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Descriptors.FieldDescriptor;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.ingest.operations.KafkaToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.ListPluginsCommand;
import mil.nga.giat.geowave.core.ingest.operations.LocalToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.LocalToHdfsCommand;
import mil.nga.giat.geowave.core.ingest.operations.LocalToKafkaCommand;
import mil.nga.giat.geowave.core.ingest.operations.LocalToMapReduceToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.MapReduceToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.SparkToGeowaveCommand;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import mil.nga.giat.geowave.service.grpc.protobuf.CoreIngestGrpc.CoreIngestImplBase;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;

public class GeoWaveGrpcCoreIngestService extends
		CoreIngestImplBase implements
		GeoWaveGrpcServiceSpi
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCoreIngestService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void localToHdfsCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.LocalToHdfsCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		LocalToHdfsCommand cmd = new LocalToHdfsCommand();
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

		LOGGER.info("Executing LocalToHdfsCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void sparkToGeowaveCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.SparkToGeowaveCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		SparkToGeowaveCommand cmd = new SparkToGeowaveCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.SetGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		if (!cmd.prepare(params)) {
			LOGGER.error("Failed to prepare parameters for SparkToGeowaveCommand");
		}

		LOGGER.info("Executing SparkToGeowaveCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void kafkaToGeowaveCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.KafkaToGeowaveCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		KafkaToGeowaveCommand cmd = new KafkaToGeowaveCommand();
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

		LOGGER.info("Executing KafkaToGeowaveCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void listPluginsCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ListPluginsCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ListPluginsCommand cmd = new ListPluginsCommand();
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

		LOGGER.info("Executing ListPluginsCommand...");
		try {
			String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
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
	public void localToKafkaCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.LocalToKafkaCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		LocalToKafkaCommand cmd = new LocalToKafkaCommand();
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

		LOGGER.info("Executing LocalToKafkaCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void localToMapReduceToGeowaveCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.LocalToMapReduceToGeowaveCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		LocalToMapReduceToGeowaveCommand cmd = new LocalToMapReduceToGeowaveCommand();
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

		LOGGER.info("Executing LocalToMapReduceToGeowaveCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void localToGeowaveCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.LocalToGeowaveCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		LocalToGeowaveCommand cmd = new LocalToGeowaveCommand();
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

		LOGGER.info("Executing LocalToGeowaveCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void mapReduceToGeowaveCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.MapReduceToGeowaveCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		MapReduceToGeowaveCommand cmd = new MapReduceToGeowaveCommand();
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

		LOGGER.info("Executing MapReduceToGeowaveCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
