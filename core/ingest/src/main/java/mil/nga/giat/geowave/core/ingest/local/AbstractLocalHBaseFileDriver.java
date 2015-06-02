/**
 * 
 */
package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import mil.nga.giat.geowave.core.ingest.AbstractIngestHBaseCommandLineDriver;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> AbstractLocalFileDriver </code>
 */
abstract public class AbstractLocalHBaseFileDriver<P extends LocalPluginBase, R> extends
		AbstractIngestHBaseCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(AbstractLocalHBaseFileDriver.class);
	protected LocalInputCommandLineOptions localInput;

	public AbstractLocalHBaseFileDriver(
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
				new LocalPluginHBaseFileVisitor<P, R>(
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
