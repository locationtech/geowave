package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import mil.nga.giat.geowave.core.ingest.AbstractIngestCommandLineDriver;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This class can be sub-classed to handle recursing over a local directory
 * structure and passing along the plugin specific handling of any supported
 * file for a discovered plugin.
 * 
 * @param <P>
 *            The type of the plugin this driver supports.
 * @param <R>
 *            The type for intermediate data that can be used throughout the
 *            life of the process and is passed along for each call to process a
 *            file.
 */
abstract public class AbstractLocalFileDriver<P extends LocalPluginBase, R> extends
		AbstractIngestCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(AbstractLocalFileDriver.class);
	protected LocalInputCommandLineOptions localInput;

	public AbstractLocalFileDriver(
			final String operation ) {
		super(
				operation);
	}

	protected void processInput(
			final Map<String, P> localPlugins,
			final R runData )
			throws IOException {
		if (localInput.getInput() == null) {
			LOGGER.fatal("Unable to ingest data, base directory or file input not specified");
			return;
		}
		final File f = new File(
				localInput.getInput());
		if (!f.exists()) {
			LOGGER.fatal("Input file '" + f.getAbsolutePath() + "' does not exist");
			throw new IllegalArgumentException(
					localInput.getInput() + " does not exist");
		}
		final File base = f.isDirectory() ? f : f.getParentFile();

		for (final LocalPluginBase localPlugin : localPlugins.values()) {
			localPlugin.init(base);
		}
		Files.walkFileTree(
				Paths.get(localInput.getInput()),
				new LocalPluginFileVisitor<P, R>(
						localPlugins,
						this,
						runData,
						localInput.getExtensions()));
	}

	abstract protected void processFile(
			final File file,
			String typeName,
			P plugin,
			R runData )
			throws IOException;

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		localInput = LocalInputCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		LocalInputCommandLineOptions.applyOptions(allOptions);
	}

}
