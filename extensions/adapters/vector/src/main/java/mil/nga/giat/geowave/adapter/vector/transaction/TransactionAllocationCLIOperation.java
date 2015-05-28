package mil.nga.giat.geowave.adapter.vector.transaction;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginConfig;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginException;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class TransactionAllocationCLIOperation implements
		CLIOperationDriver
{
	/** Package logger */
	private final static Logger LOGGER = Logger.getLogger(TransactionAllocationCLIOperation.class);

	private static void printHelp(
			final Options options ) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"GeoWaveGTDateStore",
				"\nOptions:",
				options,
				"");
	}

	public static void main(
			final String args[] )
			throws ParseException,
			GeoWavePluginException,
			IOException,
			AccumuloException,
			AccumuloSecurityException {
		final Options options = new Options();
		final OptionGroup baseOptionGroup = new OptionGroup();
		baseOptionGroup.setRequired(false);
		baseOptionGroup.addOption(new Option(
				"h",
				"help",
				false,
				"Display help"));
		options.addOptionGroup(baseOptionGroup);
		options.addOption(new Option(
				"m",
				"maximum",
				true,
				"Maximum number of simultaneous transactions"));
		options.addOption(new Option(
				"r",
				"recipient",
				true,
				"Recipient application user account for the set of transactions"));
		GeoWavePluginConfig.applyOptions(options);

		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				args);
		if (commandLine.hasOption("h")) {
			printHelp(options);
			System.exit(0);
		}
		else {
			try {
				final GeoWavePluginConfig plugin = GeoWavePluginConfig.buildFromOptions(commandLine);
				final int maximum = Integer.parseInt(commandLine.getOptionValue('m'));
				final GeoWaveGTDataStore dataStore = new GeoWaveGTDataStore(
						plugin);
				((ZooKeeperTransactionsAllocater) dataStore.getTransactionsAllocater()).preallocateTransactionIDs(
						maximum,
						commandLine.getOptionValue('r'));
			}
			catch (final Exception ex) {
				LOGGER.error(
						"Failed to pre-allocate transaction ID set",
						ex);
				System.exit(-1);
			}
		}
	}

	@Override
	public void run(
			final String[] args )
			throws ParseException {
		try {
			main(args);
		}
		catch (GeoWavePluginException | IOException | AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to run transaction allocation",
					e);
		}
	}
}