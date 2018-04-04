package mil.nga.giat.geowave.service.grpc;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import mil.nga.giat.geowave.service.grpc.protobuf.CoreIngestGrpc.CoreIngestImplBase;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;

public class GeoWaveGrpcCoreIngestService extends
		CoreIngestImplBase
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCoreIngestService.class.getName());

	@Override
	public void localToHdfsCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.LocalToHdfsCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		LocalToHdfsCommand cmd = new LocalToHdfsCommand();
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

		LOGGER.info("Executing LocalToHdfsCommand...");
		try {
			cmd.executeService(
					params).getValue();
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
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		SparkToGeowaveCommand cmd = new SparkToGeowaveCommand();
		Map<com.google.protoshadebuf3.Descriptors.FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.SetGrpcToCommandFields(
				m,
				cmd);

		final File configFile = ConfigOptions.getDefaultPropertyFile();
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		if (!cmd.prepare(params)) {
			LOGGER.error("Failed to prepare parameters for SparkToGeowaveCommand");
		}

		LOGGER.info("Executing SparkToGeowaveCommand...");
		try {
			cmd.executeService(
					params).getValue();
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
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		KafkaToGeowaveCommand cmd = new KafkaToGeowaveCommand();
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

		LOGGER.info("Executing KafkaToGeowaveCommand...");
		try {
			cmd.executeService(
					params).getValue();
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
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse> responseObserver ) {

		ListPluginsCommand cmd = new ListPluginsCommand();
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

		LOGGER.info("Executing ListPluginsCommand...");
		try {
			final List<String> result = cmd.executeService(
					params).getValue();
			final RepeatedStringResponse resp = RepeatedStringResponse.newBuilder().addAllResponseValue(
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
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		LocalToKafkaCommand cmd = new LocalToKafkaCommand();
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

		LOGGER.info("Executing LocalToKafkaCommand...");
		try {
			cmd.executeService(
					params).getValue();
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
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		LocalToMapReduceToGeowaveCommand cmd = new LocalToMapReduceToGeowaveCommand();
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

		LOGGER.info("Executing LocalToMapReduceToGeowaveCommand...");
		try {
			cmd.executeService(
					params).getValue();
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
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		LocalToGeowaveCommand cmd = new LocalToGeowaveCommand();
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

		LOGGER.info("Executing LocalToGeowaveCommand...");
		try {
			cmd.executeService(
					params).getValue();
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
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		MapReduceToGeowaveCommand cmd = new MapReduceToGeowaveCommand();
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

		LOGGER.info("Executing MapReduceToGeowaveCommand...");
		try {
			cmd.executeService(
					params).getValue();
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
