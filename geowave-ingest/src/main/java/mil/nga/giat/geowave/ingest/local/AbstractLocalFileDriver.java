package mil.nga.giat.geowave.ingest.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import mil.nga.giat.geowave.ingest.AbstractCommandLineDriver;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

abstract public class AbstractLocalFileDriver<P extends LocalPluginBase, R> extends
		AbstractCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(AbstractLocalFileDriver.class);
	protected LocalInputCommandLineOptions localInput;

	public AbstractLocalFileDriver() {
		super();
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
		for (final LocalPluginBase localPlugin : localPlugins.values()) {
			localPlugin.init(f);
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
			R runData );

	@Override
	public void parseOptions(
			final CommandLine commandLine ) {
		localInput = LocalInputCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void applyOptions(
			final Options allOptions ) {
		LocalInputCommandLineOptions.applyOptions(allOptions);
	}

}
