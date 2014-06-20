package mil.nga.giat.geowave.ingest;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

abstract public class AbstractCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(AbstractCommandLineDriver.class);
	final protected Map<String, IngestTypePluginProviderSpi<?, ?>> pluginProviderRegistry;

	public AbstractCommandLineDriver() {
		super();
		pluginProviderRegistry = new HashMap<String, IngestTypePluginProviderSpi<?, ?>>();
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

	protected List<IngestTypePluginProviderSpi<?, ?>> applyArguments(
			final String[] args )
			throws ParseException {
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
				"List the plugin provider names that can be used as the ingest type"));
		baseOptionGroup.addOption(new Option(
				"p",
				"plugins",
				true,
				"Explicitly set the ingest type by plugin provider name, if not set all available ingest types will be used"));
		options.addOptionGroup(baseOptionGroup);
		applyOptions(options);
		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				args);
		if (commandLine.hasOption("h")) {
			final HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(
					"ingest <options>",
					options);
			System.exit(0);
		}
		else if (commandLine.hasOption("l")) {
			final HelpFormatter formatter = new HelpFormatter();
			final PrintWriter pw = new PrintWriter(
					System.out);
			for (final Entry<String, IngestTypePluginProviderSpi<?, ?>> pluginProviderEntry : pluginProviderRegistry.entrySet()) {
				final IngestTypePluginProviderSpi<?, ?> pluginProvider = pluginProviderEntry.getValue();
				final String desc = pluginProvider.getIngestTypeDescription() == null ? "no description" : pluginProvider.getIngestTypeDescription();
				final String text = pluginProviderEntry.getKey() + ":\t" + desc;
				formatter.printWrapped(
						pw,
						formatter.getWidth(),
						text);
				pw.println();
			}
			pw.flush();
			System.exit(0);
		}
		else if (commandLine.hasOption("p")) {
			try {
				final String[] pluginProviderNames = commandLine.getOptionValue(
						"p").split(
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
			catch (final Exception ex) {
				System.out.println("Error parsing extensions argument, error was:");
				LOGGER.fatal(ex.getLocalizedMessage());
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
		return selectedPluginProviders;
	}

	abstract public void parseOptions(
			final CommandLine commandLine );

	abstract public void applyOptions(
			final Options allOptions );
}
