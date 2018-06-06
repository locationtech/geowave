package mil.nga.giat.geowave.service.grpc;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpcshaded.BindableService;
import io.grpcshaded.stub.StreamObserver;
import com.googleshaded.protobuf.Descriptors.FieldDescriptor;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;
import mil.nga.giat.geowave.service.grpc.protobuf.CoreMapreduceGrpc.CoreMapreduceImplBase;
import mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;

public class GeoWaveGrpcCoreMapreduceService extends
		CoreMapreduceImplBase implements
		GeoWaveGrpcServiceSpi
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCoreMapreduceService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void configHDFSCommand(
			mil.nga.giat.geowave.service.grpc.protobuf.ConfigHDFSCommandParameters request,
			StreamObserver<mil.nga.giat.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		ConfigHDFSCommand cmd = new ConfigHDFSCommand();
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

		LOGGER.info("Executing ConfigHDFSCommand...");
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
