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

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.index.StringUtils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This implements a generic command-line driven utility for discovering a set
 * of ingest format plugins and using them to drive an ingestion process. The
 * class is sub-classed to perform the specific ingestion required based on the
 * operation set by the user.
 * 
 */
abstract public class AbstractIngestCommandLineDriver implements
		CLIOperationDriver
{
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
					cleanIngestFormatName(pluginProvider.getIngestFormatName()),
					pluginProvider);
		}
	}

	private static String cleanIngestFormatName(
			String ingestFormatName ) {
		ingestFormatName = ingestFormatName.trim().toLowerCase().replaceAll(
				" ",
				"_");
		ingestFormatName = ingestFormatName.replaceAll(
				",",
				"");
		return ingestFormatName;
	}

	@Override
	public void run(
			final String[] args )
			throws ParseException {
		final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders = applyArguments(args);
		runInternal(
				args,
				pluginProviders);
	}

	@SuppressFBWarnings(value = "DM_EXIT", justification = "Exiting JVM with System.exit(0) is intentional")
	protected List<IngestFormatPluginProviderSpi<?, ?>> applyArguments(
			final String[] args ) {
		List<IngestFormatPluginProviderSpi<?, ?>> selectedPluginProviders = new ArrayList<IngestFormatPluginProviderSpi<?, ?>>();
		final Options options = new Options();
		final OptionGroup baseOptionGroup = new OptionGroup();
		baseOptionGroup.setRequired(false);
		baseOptionGroup.addOption(new Option(
				"h",
				"help",
				false,
				"Display help"));
		baseOptionGroup.addOption(new Option(
				"l",
				"list",
				false,
				"List the available ingest formats"));
		baseOptionGroup.addOption(new Option(
				"f",
				"formats",
				true,
				"Explicitly set the ingest formats by name (or multiple comma-delimited formats), if not set all available ingest formats will be used"));
		options.addOptionGroup(baseOptionGroup);
		applyOptionsInternal(options);
		final int optionCount = options.getOptions().size();
		final BasicParser parser = new BasicParser();
		try {
			CommandLine commandLine = parser.parse(
					options,
					args,
					true);
			if (commandLine.hasOption("h")) {
				printHelp(
						options,
						operation);
				System.exit(0);
			}
			else if (commandLine.hasOption("l")) {
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
				pw.flush();
				System.exit(0);
			}
			else if (commandLine.hasOption("f")) {
				try {
					selectedPluginProviders = getPluginProviders(commandLine);
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
			for (final IngestFormatPluginProviderSpi<?, ?> plugin : selectedPluginProviders) {
				final IngestFormatOptionProvider optionProvider = plugin.getIngestFormatOptionProvider();
				if (optionProvider != null) {
					optionProvider.applyOptions(options);
				}
			}
			if (options.getOptions().size() > optionCount) {
				// custom options have been added, reparse the commandline
				// arguments with the new set of options
				commandLine = parser.parse(
						options,
						args);
				for (final IngestFormatPluginProviderSpi<?, ?> plugin : selectedPluginProviders) {
					final IngestFormatOptionProvider optionProvider = plugin.getIngestFormatOptionProvider();
					if (optionProvider != null) {
						optionProvider.parseOptions(commandLine);
					}
				}
			}
			parseOptionsInternal(commandLine);
		}
		catch (final ParseException e) {
			LOGGER.fatal(e.getMessage());
			printHelp(
					options,
					operation);
			System.exit(-1);
		}
		return selectedPluginProviders;
	}

	private List<IngestFormatPluginProviderSpi<?, ?>> getPluginProviders(
			final CommandLine commandLine ) {
		final List<IngestFormatPluginProviderSpi<?, ?>> selectedPluginProviders = new ArrayList<IngestFormatPluginProviderSpi<?, ?>>();
		final String[] pluginProviderNames = commandLine.getOptionValue(
				"f").split(
				",");
		for (final String pluginProviderName : pluginProviderNames) {
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

	private static void printHelp(
			final Options options,
			final String operation ) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"-" + operation + " <options>",
				"\nOptions:",
				options,
				"");
	}

	abstract protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException;

	abstract protected void applyOptionsInternal(
			final Options allOptions );

	abstract protected void runInternal(
			String[] args,
			List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders );
}
