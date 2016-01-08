package mil.nga.giat.geowave.datastore.accumulo.split;

import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This class encapsulates all of the options and parsed values specific to
 * setting up GeoWave to appropriately connect to Accumulo. This class also can
 * perform the function of clearing data for a namespace if that option is
 * activated.
 * 
 */
public class AccumuloCommandLineOptions
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloCommandLineOptions.class);
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
		final String zookeepers = commandLine.getOptionValue(BasicAccumuloOperations.ZOOKEEPER_CONFIG_NAME);
		final String instanceId = commandLine.getOptionValue(BasicAccumuloOperations.INSTANCE_CONFIG_NAME);
		final String user = commandLine.getOptionValue(BasicAccumuloOperations.USER_CONFIG_NAME);
		final String password = commandLine.getOptionValue(BasicAccumuloOperations.PASSWORD_CONFIG_NAME);
		final String namespace = commandLine.getOptionValue(
				GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY,
				"");

		if (zookeepers == null) {
			success = false;
			LOGGER.fatal("Zookeeper URL not set");
		}
		if (instanceId == null) {
			success = false;
			LOGGER.fatal("Accumulo instance ID not set");
		}
		if (user == null) {
			success = false;
			LOGGER.fatal("Accumulo user ID not set");
		}
		if (password == null) {
			success = false;
			LOGGER.fatal("Accumulo password not set");
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
			LOGGER.fatal(
					"Unable to connect to Accumulo with the specified options",
					e);
		}
		return null;
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option zookeeperUrl = new Option(
				BasicAccumuloOperations.ZOOKEEPER_CONFIG_NAME,
				true,
				"A comma-separated list of zookeeper servers that an Accumulo instance is using");
		zookeeperUrl.setRequired(true);
		allOptions.addOption(zookeeperUrl);
		final Option instanceId = new Option(
				BasicAccumuloOperations.INSTANCE_CONFIG_NAME,
				true,
				"The Accumulo instance ID");
		instanceId.setRequired(true);
		allOptions.addOption(instanceId);
		final Option user = new Option(
				BasicAccumuloOperations.USER_CONFIG_NAME,
				true,
				"A valid Accumulo user ID");
		allOptions.addOption(user);
		user.setRequired(true);
		final Option password = new Option(
				BasicAccumuloOperations.PASSWORD_CONFIG_NAME,
				true,
				"The password for the user");
		password.setRequired(true);
		allOptions.addOption(password);
		final Option namespace = new Option(
				GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY,
				true,
				"The table namespace (optional; default is no namespace)");
		namespace.setRequired(false);
		allOptions.addOption(namespace);
	}
}
