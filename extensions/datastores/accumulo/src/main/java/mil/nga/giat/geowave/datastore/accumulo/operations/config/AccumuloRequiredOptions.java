package mil.nga.giat.geowave.datastore.accumulo.operations.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

/**
 * Default, required options needed in order to execute any command for
 * Accumulo.
 */
public class AccumuloRequiredOptions extends
		StoreFactoryOptions
{

	public static final String ZOOKEEPER_CONFIG_KEY = "zookeeper";
	public static final String INSTANCE_CONFIG_KEY = "instance";
	public static final String USER_CONFIG_KEY = "user";
	public static final String PASSWORD_CONFIG_KEY = "password";

	@Parameter(names = {
		"-z",
		"--" + ZOOKEEPER_CONFIG_KEY
	}, description = "A comma-separated list of zookeeper servers that an Accumulo instance is using", required = true)
	private String zookeeper;

	@Parameter(names = {
		"-i",
		"--" + INSTANCE_CONFIG_KEY
	}, description = "The Accumulo instance ID", required = true)
	private String instance;

	@Parameter(names = {
		"-u",
		"--" + USER_CONFIG_KEY
	}, description = "A valid Accumulo user ID", required = true)
	private String user;

	@Parameter(names = {
		"-p",
		"--" + PASSWORD_CONFIG_KEY
	}, description = "The password for the user", required = true)
	private String password;

	@ParametersDelegate
	private AccumuloOptions additionalOptions = new AccumuloOptions();

	public String getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(
			String zookeeper ) {
		this.zookeeper = zookeeper;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(
			String instance ) {
		this.instance = instance;
	}

	public String getUser() {
		return user;
	}

	public void setUser(
			String user ) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(
			String password ) {
		this.password = password;
	}

	public AccumuloOptions getAdditionalOptions() {
		return additionalOptions;
	}

	public void setAdditionalOptions(
			AccumuloOptions additionalOptions ) {
		this.additionalOptions = additionalOptions;
	}
}
