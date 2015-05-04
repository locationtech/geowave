package mil.nga.giat.geowave.core.cli;

import org.apache.commons.cli.ParseException;

public interface CLIOperationDriver
{
	public void run(
			final String[] args )
			throws ParseException;
}
