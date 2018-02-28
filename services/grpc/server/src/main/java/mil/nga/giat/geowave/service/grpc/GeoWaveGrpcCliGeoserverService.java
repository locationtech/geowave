package mil.nga.giat.geowave.service.grpc;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.cli.geoserver.ConfigGeoServerCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddCoverageCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddCoverageStoreCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddDatastoreCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddFeatureLayerCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddLayerCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddStyleCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddWorkspaceCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerGetCoverageCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerGetCoverageStoreCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerGetDatastoreCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerGetFeatureLayerCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerGetStoreAdapterCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerGetStyleCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerListCoverageStoresCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerListCoveragesCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerListDatastoresCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerListFeatureLayersCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerListStylesCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerListWorkspacesCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRemoveCoverageCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRemoveCoverageStoreCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRemoveDatastoreCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRemoveFeatureLayerCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRemoveStyleCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRemoveWorkspaceCommand;
import mil.nga.giat.geowave.cli.geoserver.GeoServerSetLayerStyleCommand;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.service.grpc.protobuf.CliGeoserverGrpc.CliGeoserverImplBase;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;

public class GeoWaveGrpcCliGeoserverService extends
		CliGeoserverImplBase
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCliGeoserverService.class.getName());

	@Override
	public void geoServerListWorkspacesCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerListWorkspacesCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse> responseObserver ) {

		GeoServerListWorkspacesCommand cmd = new GeoServerListWorkspacesCommand();
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

		LOGGER.info("Executing GeoServerListWorkspacesCommand...");
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
	public void geoServerAddCoverageCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerAddCoverageCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddCoverageCommand cmd = new GeoServerAddCoverageCommand();
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

		LOGGER.info("Executing GeoServerAddCoverageCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerRemoveCoverageStoreCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerRemoveCoverageStoreCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveCoverageStoreCommand cmd = new GeoServerRemoveCoverageStoreCommand();
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

		LOGGER.info("Executing GeoServerRemoveCoverageStoreCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerAddCoverageStoreCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerAddCoverageStoreCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddCoverageStoreCommand cmd = new GeoServerAddCoverageStoreCommand();
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

		LOGGER.info("Executing GeoServerAddCoverageStoreCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerGetCoverageStoreCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerGetCoverageStoreCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {
		GeoServerGetCoverageStoreCommand cmd = new GeoServerGetCoverageStoreCommand();
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

		LOGGER.info("Executing GeoServerGetCoverageStoreCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerAddDatastoreCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerAddDatastoreCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {
		GeoServerAddDatastoreCommand cmd = new GeoServerAddDatastoreCommand();
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

		LOGGER.info("Executing GeoServerAddDatastoreCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerGetStyleCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerGetStyleCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerGetStyleCommand cmd = new GeoServerGetStyleCommand();
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

		LOGGER.info("Executing GeoServerGetStyleCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void configGeoServerCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ConfigGeoServerCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ConfigGeoServerCommand cmd = new ConfigGeoServerCommand();
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

		LOGGER.info("Executing ConfigGeoServerCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerGetCoverageCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerGetCoverageCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {
		GeoServerGetCoverageCommand cmd = new GeoServerGetCoverageCommand();
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

		LOGGER.info("Executing GeoServerGetCoverageCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerListFeatureLayersCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerListFeatureLayersCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListFeatureLayersCommand cmd = new GeoServerListFeatureLayersCommand();
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

		LOGGER.info("Executing GeoServerListFeatureLayersCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerGetStoreAdapterCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerGetStoreAdapterCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse> responseObserver ) {

		GeoServerGetStoreAdapterCommand cmd = new GeoServerGetStoreAdapterCommand();
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

		LOGGER.info("Executing GeoServerGetStoreAdapterCommand...");
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
	public void geoServerAddWorkspaceCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerAddWorkspaceCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {
		GeoServerAddWorkspaceCommand cmd = new GeoServerAddWorkspaceCommand();
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

		LOGGER.info("Executing GeoServerAddWorkspaceCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerRemoveDatastoreCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerRemoveDatastoreCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveDatastoreCommand cmd = new GeoServerRemoveDatastoreCommand();
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

		LOGGER.info("Executing GeoServerRemoveDatastoreCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerRemoveWorkspaceCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerRemoveWorkspaceCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveWorkspaceCommand cmd = new GeoServerRemoveWorkspaceCommand();
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

		LOGGER.info("Executing GeoServerRemoveWorkspaceCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerAddStyleCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerAddStyleCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddStyleCommand cmd = new GeoServerAddStyleCommand();
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

		LOGGER.info("Executing GeoServerAddStyleCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerListDatastoresCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerListDatastoresCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListDatastoresCommand cmd = new GeoServerListDatastoresCommand();
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

		LOGGER.info("Executing GeoServerListDatastoresCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerListCoverageStoresCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerListCoverageStoresCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListCoverageStoresCommand cmd = new GeoServerListCoverageStoresCommand();
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

		LOGGER.info("Executing GeoServerListCoverageStoresCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerAddLayerCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerAddLayerCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddLayerCommand cmd = new GeoServerAddLayerCommand();
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

		LOGGER.info("Executing GeoServerAddLayerCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerListStylesCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerListStylesCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListStylesCommand cmd = new GeoServerListStylesCommand();
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

		LOGGER.info("Executing GeoServerListStylesCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerGetFeatureLayerCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerGetFeatureLayerCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerGetFeatureLayerCommand cmd = new GeoServerGetFeatureLayerCommand();
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

		LOGGER.info("Executing GeoServerGetFeatureLayerCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerRemoveCoverageCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerRemoveCoverageCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveCoverageCommand cmd = new GeoServerRemoveCoverageCommand();
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

		LOGGER.info("Executing GeoServerRemoveCoverageCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerListCoveragesCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerListCoveragesCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListCoveragesCommand cmd = new GeoServerListCoveragesCommand();
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

		LOGGER.info("Executing GeoServerListCoveragesCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerRemoveFeatureLayerCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerRemoveFeatureLayerCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveFeatureLayerCommand cmd = new GeoServerRemoveFeatureLayerCommand();
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

		LOGGER.info("Executing GeoServerRemoveFeatureLayerCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerRemoveStyleCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerRemoveStyleCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveStyleCommand cmd = new GeoServerRemoveStyleCommand();
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

		LOGGER.info("Executing GeoServerRemoveStyleCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerGetDatastoreCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerGetDatastoreCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerGetDatastoreCommand cmd = new GeoServerGetDatastoreCommand();
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

		LOGGER.info("Executing GeoServerGetDatastoreCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerAddFeatureLayerCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerAddFeatureLayerCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddFeatureLayerCommand cmd = new GeoServerAddFeatureLayerCommand();
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

		LOGGER.info("Executing GeoServerAddFeatureLayerCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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
	public void geoServerSetLayerStyleCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.GeoServerSetLayerStyleCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerSetLayerStyleCommand cmd = new GeoServerSetLayerStyleCommand();
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

		LOGGER.info("Executing GeoServerSetLayerStyleCommand...");
		try {
			final String result = cmd.executeService(
					params).getValue();
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

}
