package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.ingest.DataAdapterProvider;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

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
abstract public class AbstractLocalFileDriver<P extends LocalPluginBase, R>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractLocalFileDriver.class);
	protected LocalInputCommandLineOptions localInput;

	public AbstractLocalFileDriver(
			LocalInputCommandLineOptions input ) {
		localInput = input;
	}

	protected boolean checkIndexesAgainstProvider(
			String providerName,
			DataAdapterProvider<?> adapterProvider,
			List<IndexPluginOptions> indexOptions ) {
		boolean valid = true;
		for (IndexPluginOptions option : indexOptions) {
			if (!IngestUtils.isCompatible(
					adapterProvider,
					option)) {
				// HP Fortify "Log Forging" false positive
				// What Fortify considers "user input" comes only
				// from users with OS-level access anyway
				LOGGER.warn("Local file ingest plugin for ingest type '" + providerName
						+ "' does not support dimensionality '" + option.getType() + "'");
				valid = false;
			}
		}
		return valid;
	}

	protected void processInput(
			final String inputPath,
			final Map<String, P> localPlugins,
			final R runData )
			throws IOException {
		if (inputPath == null) {
			LOGGER.error("Unable to ingest data, base directory or file input not specified");
			return;
		}
		final File f = new File(
				inputPath);
		if (!f.exists()) {
			LOGGER.error("Input file '" + f.getAbsolutePath() + "' does not exist");
			throw new IllegalArgumentException(
					inputPath + " does not exist");
		}
		final File base = f.isDirectory() ? f : f.getParentFile();

		for (final LocalPluginBase localPlugin : localPlugins.values()) {
			localPlugin.init(base);
		}
		Files.walkFileTree(
				Paths.get(inputPath),
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
}
