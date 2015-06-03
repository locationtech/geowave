package mil.nga.giat.geowave.datastore.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates all of the options and parsed values specific to
 * setting up GeoWave to appropriately connect to Accumulo.
 * 
 */
public class AccumuloCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloCommandLineOptions.class);
	private final String zookeepers;
	private final String instanceId;
	private final String user;
	private final String password;
	private final String namespace;
	private AccumuloOperations operations;

	public AccumuloCommandLineOptions(
			final String zookeepers,
			final String instanceId,
			final String user,
			final String password,
			final String namespace )
			throws AccumuloException,
			AccumuloSecurityException {
		this.zookeepers = zookeepers;
		this.instanceId = instanceId;
		this.user = user;
		this.password = password;
		this.namespace = namespace;

	}

	public String getZookeepers() {
		return zookeepers;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String getNamespace() {
		return namespace;
	}

	public synchronized AccumuloOperations getAccumuloOperations()
			throws AccumuloException,
			AccumuloSecurityException {
		if (operations == null) {
			operations = new BasicAccumuloOperations(
					zookeepers,
					instanceId,
					user,
					password,
					namespace);
		}
		return operations;
	}

	public static AccumuloCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		boolean success = true;
		final String zookeepers = commandLine.getOptionValue("z");
		final String instanceId = commandLine.getOptionValue("i");
		final String user = commandLine.getOptionValue("u");
		final String password = commandLine.getOptionValue("p");
		final String namespace = commandLine.getOptionValue(
				"n",
				"");
		if (zookeepers == null) {
			success = false;
			LOGGER.error("Zookeeper URL not set");
		}
		if (instanceId == null) {
			success = false;
			LOGGER.error("Accumulo instance ID not set");
		}
		if (user == null) {
			success = false;
			LOGGER.error("Accumulo user ID not set");
		}
		if (password == null) {
			success = false;
			LOGGER.error("Accumulo password not set");
		}
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}
		try {
			return new AccumuloCommandLineOptions(
					zookeepers,
					instanceId,
					user,
					password,
					namespace);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to connect to Accumulo with the specified options",
					e);
		}
		return null;
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
}
