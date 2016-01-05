package mil.nga.giat.geowave.core.ingest;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;

import org.apache.commons.cli.HelpFormatter;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This implements a generic command-line driven utility for discovering a set
 * of ingest format plugins and using them to drive an ingestion process. The
 * class is sub-classed to perform the specific ingestion required based on the
 * operation set by the user.
 *
 */
abstract public class AbstractIngestCommandLineDriver implements
		CLIOperation
{
	@Parameter(names = "--list-formats", description = "List the available ingest formats", help = true)
	private final boolean listFormats = false;
	@Parameter(names = {
		"-f",
		"-format"
	}, description = "Explicitly set the ingest formats by name (or multiple comma-delimited formats), if not set all available ingest formats will be used")
	private final List<String> formats = new ArrayList<String>();

	private final static Logger LOGGER = Logger.getLogger(AbstractIngestCommandLineDriver.class);
	final protected Map<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderRegistry;
	private final String operation;

	public AbstractIngestCommandLineDriver(
			final String operation ) {
		super();
		pluginProviderRegistry = new HashMap<String, IngestFormatPluginProviderSpi<?, ?>>();
		this.operation = operation;
		initPluginProviderRegistry();
	}

	private void initPluginProviderRegistry() {
		final Iterator<IngestFormatPluginProviderSpi> pluginProviders = ServiceLoader.load(
				IngestFormatPluginProviderSpi.class).iterator();
		while (pluginProviders.hasNext()) {
			final IngestFormatPluginProviderSpi pluginProvider = pluginProviders.next();
			pluginProviderRegistry.put(
					ConfigUtils.cleanOptionName(pluginProvider.getIngestFormatName()),
					pluginProvider);
		}
	}

	@Override
	public boolean doOperation(
			final JCommander commander ) {
		if (listFormats) {
			printFormats(pluginProviderRegistry);
			return true;
		}
		final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders = processFormatPlugins(commander);
		final boolean retVal = runInternal(pluginProviders);
		return retVal;
	}

	@SuppressFBWarnings(value = "DM_EXIT", justification = "Exiting JVM with System.exit(0) is intentional")
	protected List<IngestFormatPluginProviderSpi<?, ?>> processFormatPlugins(
			final JCommander commander ) {
		List<IngestFormatPluginProviderSpi<?, ?>> selectedPluginProviders = new ArrayList<IngestFormatPluginProviderSpi<?, ?>>();
		initInternal(commander);
		if (!formats.isEmpty()) {
			try {
				selectedPluginProviders = getPluginProviders();
			}
			catch (final Exception e) {
				LOGGER.fatal(
						"Error parsing plugins",
						e);
				System.exit(-3);
			}
		}
		else {
			selectedPluginProviders.addAll(pluginProviderRegistry.values());
			if (selectedPluginProviders.isEmpty()) {
				LOGGER.fatal("There were no ingest format plugin providers found");
				System.exit(-3);
			}
		}
		applyAdditionalOptions(
				selectedPluginProviders,
				commander);
		return selectedPluginProviders;
	}

	private static void printFormats(
			final Map<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderRegistry ) {
		final HelpFormatter formatter = new HelpFormatter();
		final PrintWriter pw = new PrintWriter(
				new OutputStreamWriter(
						System.out,
						StringUtils.UTF8_CHAR_SET));
		pw.println("Available ingest formats currently registered as plugins:\n");
		for (final Entry<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderEntry : pluginProviderRegistry.entrySet()) {
			final IngestFormatPluginProviderSpi<?, ?> pluginProvider = pluginProviderEntry.getValue();
			final String desc = pluginProvider.getIngestFormatDescription() == null ? "no description" : pluginProvider.getIngestFormatDescription();
			final String text = pluginProviderEntry.getKey() + ":\n" + desc;

			formatter.printWrapped(
					pw,
					formatter.getWidth(),
					5,
					text);
			pw.println();
		}
	}

	private boolean applyAdditionalOptions(
			final List<IngestFormatPluginProviderSpi<?, ?>> selectedPluginProviders,
			final JCommander commander ) {
		boolean retVal = false;
		final JCommander additionalParameters = new JCommander();
		for (final IngestFormatPluginProviderSpi<?, ?> plugin : selectedPluginProviders) {
			final Object[] additionalOptions = plugin.getIngestFormatOptions();
			if ((additionalOptions != null) && (additionalOptions.length > 0)) {
				retVal = true;
				for (final Object option : additionalOptions) {
					additionalParameters.addObject(option);
				}
			}
		}
		additionalParameters.setAcceptUnknownOptions(true);
		additionalParameters.parse(commander.getUnknownOptions().toArray(
				new String[] {}));
		return retVal;
		// final DataStoreFactorySpi dataStoreFactory =
		// DataStoreCommandLineOptions.getSelectedStore(new CommandLineWrapper(
		// commandLine));
		// if (dataStoreFactory != null) {
		// GenericStoreCommandLineOptions.applyStoreOptions(
		// dataStoreFactory,
		// options);
		// }
	}

	private List<IngestFormatPluginProviderSpi<?, ?>> getPluginProviders() {
		final List<IngestFormatPluginProviderSpi<?, ?>> selectedPluginProviders = new ArrayList<IngestFormatPluginProviderSpi<?, ?>>();
		for (final String pluginProviderName : formats) {
			final IngestFormatPluginProviderSpi<?, ?> pluginProvider = pluginProviderRegistry.get(pluginProviderName);
			if (pluginProvider == null) {
				throw new IllegalArgumentException(
						"Unable to find SPI plugin provider for ingest format '" + pluginProviderName + "'");
			}
			selectedPluginProviders.add(pluginProvider);
		}
		if (selectedPluginProviders.isEmpty()) {
			throw new IllegalArgumentException(
					"There were no ingest format plugin providers found");
		}
		return selectedPluginProviders;
	}

	//
	// private static void printHelp(
	// final Options options,
	// final String operation ) {
	// final HelpFormatter formatter = new HelpFormatter();
	// formatter.printHelp(
	// "-" + operation + " <options>",
	// "\nOptions:",
	// options,
	// "");
	// }

	@Override
	public void init(
			final JCommander commander ) {

	}

	abstract protected boolean initInternal(
			final JCommander commander );

	abstract protected boolean runInternal(
			List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders );
}
