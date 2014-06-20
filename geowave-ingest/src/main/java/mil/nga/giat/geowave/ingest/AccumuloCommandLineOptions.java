package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class AccumuloCommandLineOptions
{
	private final String zookeepers;
	private final String instanceId;
	private final String user;
	private final String password;
	private final String namespace;
	private final String visibility;
	private final boolean clearNamespace;
	private final IndexType type;

	public AccumuloCommandLineOptions(
			final String zookeepers,
			final String instanceId,
			final String user,
			final String password,
			final String namespace,
			final String visibility,
			final boolean clearNamespace,
			final IndexType type ) {
		this.zookeepers = zookeepers;
		this.instanceId = instanceId;
		this.user = user;
		this.password = password;
		this.namespace = namespace;
		this.visibility = visibility;
		this.clearNamespace = clearNamespace;
		this.type = type;
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

	public IndexType getType() {
		return type;
	}

	public boolean isClearNamespace() {
		return clearNamespace;
	}

	public Index getIndex() {
		return type.createDefaultIndex();
	}

	public AccumuloOperations getAccumuloOperations()
			throws AccumuloException,
			AccumuloSecurityException {
		return new BasicAccumuloOperations(
				zookeepers,
				instanceId,
				user,
				password,
				namespace);
	}

	public static AccumuloCommandLineOptions parseOptions(
			final CommandLine commandLine ) {
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
				"t",
				"spatial");
		IndexType type = IndexType.SPATIAL;
		if (typeValue.equalsIgnoreCase("spatial-temporal")) {
			type = IndexType.SPATIAL_TEMPORAL;
		}
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

	public static void applyOptions(
			final Options allOptions ) {
		final Option zookeeperUrl = new Option(
				"z",
				"zookeepers",
				true,
				"A comma-separated list of zookeeper servers that an Accumulo instance is using");
		zookeeperUrl.setRequired(true);
		allOptions.addOption(zookeeperUrl);
		final Option instanceId = new Option(
				"i",
				"instance-id",
				true,
				"The Accumulo instance ID");
		instanceId.setRequired(true);
		allOptions.addOption(instanceId);
		final Option user = new Option(
				"u",
				"user",
				true,
				"A valid Accumulo user ID");
		user.setRequired(true);
		allOptions.addOption(user);
		final Option password = new Option(
				"p",
				"password",
				true,
				"The password for the user");
		password.setRequired(true);
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
				"t",
				"type",
				true,
				"The type of index, either 'spatial' or 'spatial-temporal' (optional; default is 'spatial')");
		allOptions.addOption(indexType);
		allOptions.addOption(new Option(
				"c",
				"clear",
				false,
				"Clear ALL data stored with the same prefix as this namespace"));
	}
}
