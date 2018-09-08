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
		"-b",
		"--blockUntilShutdown"
	}, required = false, description = "Should the service run until manual shutdown?")
	private Boolean blockUntilShutdown = true;

	public Integer getPort() {
		return port;
	}
	
	public Boolean getBlockUntilShutdown() {
		return blockUntilShutdown;
	}

	public void setPort(
			final Integer p ) {
		port = p;
	}
	
	public void setBlockUntilShutdown(final Boolean b) {
		blockUntilShutdown = b;
	}
}