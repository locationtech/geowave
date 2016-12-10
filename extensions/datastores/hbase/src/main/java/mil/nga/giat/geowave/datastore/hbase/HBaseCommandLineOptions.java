package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class HBaseCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseCommandLineOptions.class);
	private final String zookeepers;
	private final String namespace;
	private final boolean bigtable;
	private BasicHBaseOperations operations;

	public HBaseCommandLineOptions(
			final String zookeepers,
			final String namespace,
			final boolean bigtable ) {
		this.zookeepers = zookeepers;
		this.namespace = namespace;
		this.bigtable = bigtable;
	}

	public String getZookeepers() {
		return zookeepers;
	}

	public String getNamespace() {
		return namespace;
	}

	public synchronized BasicHBaseOperations getHBaseOperations()
			throws IOException {
		if (operations == null) {
			operations = new BasicHBaseOperations(
					zookeepers,
					namespace,
					bigtable);
		}
		return operations;
	}

	public static HBaseCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final String zookeepers = commandLine.getOptionValue("z");
		final String namespace = commandLine.getOptionValue(
				"n",
				"");
		final String bigtableStr = commandLine.getOptionValue("bigtable");
		boolean bigtable = false;
		if (bigtableStr != null) {
			bigtable = Boolean.parseBoolean(bigtableStr);
		}

		if (zookeepers == null) {
			LOGGER.error("Zookeeper URL not set");
			throw new ParseException(
					"Required option is missing");
		}

		return new HBaseCommandLineOptions(
				zookeepers,
				namespace,
				bigtable);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option zookeeperUrl = new Option(
				"z",
				"zookeepers",
				true,
				"A comma-separated list of zookeeper servers that an Accumulo instance is using");
		allOptions.addOption(zookeeperUrl);
		final Option instanceId = new Option(
				"i",
				"instance-id",
				true,
				"The Instance ID");
		allOptions.addOption(instanceId);
		final Option visibility = new Option(
				"v",
				"visibility",
				true,
				"The visibility of the data ingested (optional; default is 'public')");
		allOptions.addOption(visibility);

		final Option namespace = new Option(
				"n",
				"namespace",
				true,
				"The table namespace (optional; default is no namespace)");
		allOptions.addOption(namespace);
	}

	public synchronized BasicHBaseOperations getOperations()
			throws IOException {
		if (operations == null) {
			operations = new BasicHBaseOperations(
					zookeepers,
					namespace,
					bigtable);
		}
		return operations;
	}
}
