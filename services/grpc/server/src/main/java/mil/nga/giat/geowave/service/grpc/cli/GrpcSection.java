package mil.nga.giat.geowave.service.grpc.cli;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "grpc", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Commands to start/stop/restart gRPC services")
public class GrpcSection extends
		DefaultOperation
{

}