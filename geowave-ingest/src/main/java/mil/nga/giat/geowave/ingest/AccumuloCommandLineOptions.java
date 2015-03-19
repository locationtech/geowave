package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.store.index.DimensionalityType;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
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
	private final String visibility;
	private final boolean clearNamespace;
	private final DimensionalityType type;
	private AccumuloOperations operations;

	public AccumuloCommandLineOptions(
			final String zookeepers,
			final String instanceId,
			final String user,
			final String password,
			final String namespace,
			final String visibility,
			final boolean clearNamespace,
			final DimensionalityType type )
			throws AccumuloException,
			AccumuloSecurityException {
		this.zookeepers = zookeepers;
		this.instanceId = instanceId;
		this.user = user;
		this.password = password;
		this.namespace = namespace;
		this.visibility = visibility;
		this.clearNamespace = clearNamespace;
		this.type = type;

		if (clearNamespace) {
			clearNamespace();
		}
	}

	protected void clearNamespace()
			throws AccumuloException,
			AccumuloSecurityException {
		// don't delete all tables in the case that no namespace is given
		if ((namespace != null) && !namespace.isEmpty()) {
			LOGGER.info("deleting all tables prefixed by '" + namespace + "'");
			try {
				getAccumuloOperations().deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
				LOGGER.error("Unable to clear accumulo namespace");
			}

		}
		else {
			LOGGER.error("cannot clear a namespace if no namespace is provided");
		}
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

	public String getVisibility() {
		return visibility;
	}

	public DimensionalityType getType() {
		return type;
	}

	public boolean isClearNamespace() {
		return clearNamespace;
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

	public Index getIndex(
			final Index[] supportedIndices ) {
		for (final Index i : supportedIndices) {
			if (i.getDimensionalityType().equals(
					type)) {
				return i;
			}
		}
		return null;
	}

	public boolean isSupported(
			final Index[] supportedIndices ) {
		return (getIndex(supportedIndices) != null);
	}

	public static AccumuloCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		boolean success = true;
		final String zookeepers = commandLine.getOptionValue("z");
		final String instanceId = commandLine.getOptionValue("i");
		final String user = commandLine.getOptionValue("u");
		final String password = commandLine.getOptionValue("p");
		boolean clearNamespace = false;
		if (commandLine.hasOption("c")) {
			clearNamespace = true;
		}
		String visibility = null;
		if (commandLine.hasOption("v")) {
			visibility = commandLine.getOptionValue("v");
		}
		final String namespace = commandLine.getOptionValue(
				"n",
				"");
		final String typeValue = commandLine.getOptionValue(
				"dim",
				"spatial");
		DimensionalityType type = DimensionalityType.SPATIAL;
		if (typeValue.equalsIgnoreCase("spatial-temporal")) {
			type = DimensionalityType.SPATIAL_TEMPORAL;
		}
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
					namespace,
					visibility,
					clearNamespace,
					type);
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
				"The visiblity of the data ingested (optional; default is 'public')");
		allOptions.addOption(visibility);

		final Option namespace = new Option(
				"n",
				"namespace",
				true,
				"The table namespace (optional; default is no namespace)");
		allOptions.addOption(namespace);
		final Option indexType = new Option(
				"dim",
				"dimensionality",
				true,
				"The dimensionality type for the index, either 'spatial' or 'spatial-temporal' (optional; default is 'spatial')");
		allOptions.addOption(indexType);
		allOptions.addOption(new Option(
				"c",
				"clear",
				false,
				"Clear ALL data stored with the same prefix as this namespace (optional; default is to append data to the namespace if it exists)"));
	}
}
