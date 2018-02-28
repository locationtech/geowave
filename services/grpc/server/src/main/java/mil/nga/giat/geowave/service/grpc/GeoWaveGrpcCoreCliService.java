package mil.nga.giat.geowave.service.grpc;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ListCommand;
import mil.nga.giat.geowave.core.cli.operations.config.SetCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.service.grpc.protobuf.CoreCliGrpc.CoreCliImplBase;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.MapStringStringResponse;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;

public class GeoWaveGrpcCoreCliService extends
		CoreCliImplBase
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCoreCliService.class.getName());

	@Override
	public void setCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.SetCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		SetCommand cmd = new SetCommand();
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

		LOGGER.info("Executing SetCommand...");
		try {
			final String result = (String) cmd.executeService(
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
	public void listCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ListCommandParameters request,
			io.grpc.stub.StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.MapStringStringResponse> responseObserver ) {

		ListCommand cmd = new ListCommand();
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

		LOGGER.info("Executing ListCommand...");
		try {
			final Map<String, String> post_result = new HashMap<String, String>();
			final Map<String, Object> result = cmd.executeService(
					params).getValue();
			final Iterator<Entry<String, Object>> it = result.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Object> pair = (Map.Entry<String, Object>) it.next();
				post_result.put(
						pair.getKey().toString(),
						pair.getValue().toString());
			}
			final MapStringStringResponse resp = MapStringStringResponse.newBuilder().putAllResponseValue(
					post_result).build();
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
