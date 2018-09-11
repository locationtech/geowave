package mil.nga.giat.geowave.service.grpc.cli;

import com.beust.jcommander.Parameter;

public class StartGrpcServerCommandOptions
{
	@Parameter(names = {
		"-p",
		"--port"
	}, required = false, description = "The port to run on")
	private Integer port = 8980;

	@Parameter(names = {
		"-n",
		"--nonBlocking"
	}, required = false, description = "Should the service run as non-blocking or block until shutdown?")
	private Boolean nonBlocking = false;

	public Integer getPort() {
		return port;
	}

	public Boolean isNonBlocking() {
		return nonBlocking;
	}

	public void setPort(
			final Integer p ) {
		port = p;
	}

	public void setNonBlocking(
			final Boolean b ) {
		nonBlocking = b;
	}
}