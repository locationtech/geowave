package mil.nga.giat.geowave.ingest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public interface IngestTypeOptionProvider
{

	/**
	 * Add more options to the command line arguments
	 * 
	 * @param allOptions
	 *            the options to add to
	 */
	public void applyOptions(
			final Options allOptions );

	/**
	 * Parse the command line values passed in based on the custom options
	 * provided
	 * 
	 * @param commandLine
	 *            The values of the arguments to parse
	 */
	public void parseOptions(
			final CommandLine commandLine );

}
