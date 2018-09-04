package mil.nga.giat.geowave.service.grpc.services;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Descriptors.FieldDescriptor;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.cli.config.AddIndexGroupCommand;
import mil.nga.giat.geowave.core.store.cli.config.RemoveIndexCommand;
import mil.nga.giat.geowave.core.store.cli.config.RemoveIndexGroupCommand;
import mil.nga.giat.geowave.core.store.cli.config.RemoveStoreCommand;
import mil.nga.giat.geowave.core.store.cli.remote.CalculateStatCommand;
import mil.nga.giat.geowave.core.store.cli.remote.ClearCommand;
import mil.nga.giat.geowave.core.store.cli.remote.ListAdapterCommand;
import mil.nga.giat.geowave.core.store.cli.remote.ListIndexCommand;
import mil.nga.giat.geowave.core.store.cli.remote.ListStatsCommand;
import mil.nga.giat.geowave.core.store.cli.remote.RecalculateStatsCommand;
import mil.nga.giat.geowave.core.store.cli.remote.RemoveAdapterCommand;
import mil.nga.giat.geowave.core.store.cli.remote.RemoveStatCommand;
import mil.nga.giat.geowave.core.store.operations.remote.VersionCommand;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import mil.nga.giat.geowave.service.grpc.protobuf.CoreStoreGrpc.CoreStoreImplBase;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;

public class GeoWaveGrpcCoreStoreService extends
		CoreStoreImplBase implements
		GeoWaveGrpcServiceSpi
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCoreStoreService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void removeAdapterCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.RemoveAdapterCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		RemoveAdapterCommand cmd = new RemoveAdapterCommand();
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

		LOGGER.info("Executing RemoveAdapterCommand...");
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
	public void removeStoreCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.RemoveStoreCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		RemoveStoreCommand cmd = new RemoveStoreCommand();
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

		LOGGER.info("Executing RemoveStoreCommand...");
		try {
			final String result = cmd.computeResults(params);
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
	public void listAdapterCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ListAdapterCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ListAdapterCommand cmd = new ListAdapterCommand();
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

		LOGGER.info("Executing ListAdapterCommand...");
		try {
			final String result = cmd.computeResults(params);
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
	public void calculateStatCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.CalculateStatCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		CalculateStatCommand cmd = new CalculateStatCommand();
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

		LOGGER.info("Executing CalculateStatCommand...");
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
	public void removeIndexGroupCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.RemoveIndexGroupCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		RemoveIndexGroupCommand cmd = new RemoveIndexGroupCommand();
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

		LOGGER.info("Executing RemoveIndexGroupCommand...");
		try {
			final String result = cmd.computeResults(params);
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
	public void recalculateStatsCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.RecalculateStatsCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		RecalculateStatsCommand cmd = new RecalculateStatsCommand();
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

		LOGGER.info("Executing RecalculateStatsCommand...");
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
	public void listStatsCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ListStatsCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ListStatsCommand cmd = new ListStatsCommand();
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

		LOGGER.info("Executing ListStatsCommand...");
		try {
			final String result = cmd.computeResults(params);
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
	public void listIndexCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ListIndexCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ListIndexCommand cmd = new ListIndexCommand();
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

		LOGGER.info("Executing ListIndexCommand...");
		try {
			final String result = cmd.computeResults(params);
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
	public void clearCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ClearCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		ClearCommand cmd = new ClearCommand();
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

		LOGGER.info("Executing ClearCommand...");
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
	public void versionCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.VersionCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		VersionCommand cmd = new VersionCommand();
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

		LOGGER.info("Executing VersionCommand...");
		try {
			cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().build();
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
	public void addIndexGroupCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.AddIndexGroupCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		AddIndexGroupCommand cmd = new AddIndexGroupCommand();
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

		LOGGER.info("Executing AddIndexGroupCommand...");
		try {
			final String result = cmd.computeResults(params);
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
	public void removeIndexCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.RemoveIndexCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		RemoveIndexCommand cmd = new RemoveIndexCommand();
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

		LOGGER.info("Executing RemoveIndexCommand...");
		try {
			final String result = cmd.computeResults(params);
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
	public void removeStatCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.RemoveStatCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		RemoveStatCommand cmd = new RemoveStatCommand();
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

		LOGGER.info("Executing RemoveStatCommand...");
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
