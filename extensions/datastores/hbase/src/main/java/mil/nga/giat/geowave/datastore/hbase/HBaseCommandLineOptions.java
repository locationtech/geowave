package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author viggy Functionality similar to
 *         <code> AccumuloCommandLineOptions </code>
 */
public class HBaseCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseCommandLineOptions.class);
	private final String zookeepers;
	private final String namespace;
	private BasicHBaseOperations operations;

	public HBaseCommandLineOptions(
			final String zookeepers,
			final String namespace ) {
		this.zookeepers = zookeepers;
		this.namespace = namespace;

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
					namespace);
		}
		return operations;
	}

	public static HBaseCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		boolean success = true;
		final String zookeepers = commandLine.getOptionValue("z");
		final String namespace = commandLine.getOptionValue(
				"n",
				"");
		if (zookeepers == null) {
			success = false;
			LOGGER.error("Zookeeper URL not set");
		}
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}
		return new HBaseCommandLineOptions(
				zookeepers,
				namespace);
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
				"The Accumulo instance ID");
		allOptions.addOption(instanceId);
		final Option user = new Option(
				"u",
				"user",
				true,
				"A valid Accumulo user ID");
		allOptions.addOption(user);
		final Option password = new Option(
				"p",
				"password",
				true,
				"The password for the user");
		allOptions.addOption(password);
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
					namespace);
		}
		return operations;
	}
}
