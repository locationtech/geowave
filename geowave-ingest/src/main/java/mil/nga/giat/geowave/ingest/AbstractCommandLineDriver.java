package mil.nga.giat.geowave.ingest;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This implements a generic command-line driven utility for discovering a set
 * of ingest type plugins and using them to drive an ingestion process. The
 * class is sub-classed to perform the specific ingestion required based on the
 * operation set by the user.
 * 
 */
abstract public class AbstractCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(AbstractCommandLineDriver.class);
	final protected Map<String, IngestTypePluginProviderSpi<?, ?>> pluginProviderRegistry;
	private final String operation;

	public AbstractCommandLineDriver(
			final String operation ) {
		super();
		pluginProviderRegistry = new HashMap<String, IngestTypePluginProviderSpi<?, ?>>();
		this.operation = operation;
		initPluginProviderRegistry();
	}

	private void initPluginProviderRegistry() {
		final Iterator<IngestTypePluginProviderSpi> pluginProviders = ServiceLoader.load(
				IngestTypePluginProviderSpi.class).iterator();
		while (pluginProviders.hasNext()) {
			final IngestTypePluginProviderSpi pluginProvider = pluginProviders.next();
			pluginProviderRegistry.put(
					cleanIngestTypeName(pluginProvider.getIngestTypeName()),
					pluginProvider);
		}
	}

	private static String cleanIngestTypeName(
			String ingestTypeName ) {
		ingestTypeName = ingestTypeName.trim().toLowerCase().replaceAll(
				" ",
				"_");
		ingestTypeName = ingestTypeName.replaceAll(
				",",
				"");
		return ingestTypeName;
	}

	public void run(
			final String[] args )
			throws ParseException {
		final List<IngestTypePluginProviderSpi<?, ?>> pluginProviders = applyArguments(args);
		runInternal(
				args,
				pluginProviders);
	}

	abstract protected void runInternal(
			String[] args,
			List<IngestTypePluginProviderSpi<?, ?>> pluginProviders );

	@SuppressFBWarnings(value="DM_EXIT", justification = "Exiting JVM with System.exit(0) is intentional")
	protected List<IngestTypePluginProviderSpi<?, ?>> applyArguments(
			final String[] args ) {
		final List<IngestTypePluginProviderSpi<?, ?>> selectedPluginProviders = new ArrayList<IngestTypePluginProviderSpi<?, ?>>();
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
				"List the available ingest types"));
		baseOptionGroup.addOption(new Option(
				"t",
				"types",
				true,
				"Explicitly set the ingest type by name (or multiple comma-delimited types), if not set all available ingest types will be used"));
		options.addOptionGroup(baseOptionGroup);
		applyOptions(options);
		final BasicParser parser = new BasicParser();
		try {
			final CommandLine commandLine = parser.parse(
					options,
					args);
			if (commandLine.hasOption("h")) {
				printHelp(
						options,
						operation);
				System.exit(0);
			}
			else if (commandLine.hasOption("l")) {
				final HelpFormatter formatter = new HelpFormatter();
				final PrintWriter pw = new PrintWriter(
						System.out);
				pw.println("Available ingest types currently registered as plugins:\n");
				for (final Entry<String, IngestTypePluginProviderSpi<?, ?>> pluginProviderEntry : pluginProviderRegistry.entrySet()) {
					final IngestTypePluginProviderSpi<?, ?> pluginProvider = pluginProviderEntry.getValue();
					final String desc = pluginProvider.getIngestTypeDescription() == null ? "no description" : pluginProvider.getIngestTypeDescription();
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
			else if (commandLine.hasOption("t")) {
				try {
					final String[] pluginProviderNames = commandLine.getOptionValue(
							"t").split(
							",");
					for (final String pluginProviderName : pluginProviderNames) {
						final IngestTypePluginProviderSpi<?, ?> pluginProvider = pluginProviderRegistry.get(pluginProviderName);
						if (pluginProvider == null) {
							throw new IllegalArgumentException(
									"Unable to find SPI plugin provider for ingest type '" + pluginProviderName + "'");
						}
						selectedPluginProviders.add(pluginProvider);
					}
					if (selectedPluginProviders.isEmpty()) {
						throw new IllegalArgumentException(
								"There were no ingest type plugin providers found");
					}
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
					LOGGER.fatal("There were no ingest type plugin providers found");
					System.exit(-3);
				}
			}
			parseOptions(commandLine);
		}
		catch (final ParseException e) {
			LOGGER.fatal(
					"",
					e);
			printHelp(
					options,
					operation);
			System.exit(-1);
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

	abstract public void parseOptions(
			final CommandLine commandLine )
			throws ParseException;

	abstract public void applyOptions(
			final Options allOptions );
}
