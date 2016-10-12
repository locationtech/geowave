package mil.nga.giat.geowave.datastore.hbase.operations.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

public class HBaseRequiredOptions extends
		StoreFactoryOptions
{

	public static final String ZOOKEEPER_CONFIG_KEY = "zookeeper";

	@Parameter(names = {
		"-z",
		"--" + ZOOKEEPER_CONFIG_KEY
	}, description = "A comma-separated list of zookeeper servers that an HBase instance is using", required = true)
	private String zookeeper;

	@ParametersDelegate
	private HBaseOptions additionalOptions = new HBaseOptions();

	public String getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(
			String zookeeper ) {
		this.zookeeper = zookeeper;
	}

	public HBaseOptions getAdditionalOptions() {
		return additionalOptions;
	}

	public void setAdditionalOptions(
			HBaseOptions additionalOptions ) {
		this.additionalOptions = additionalOptions;
	}
}
