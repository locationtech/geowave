package mil.nga.giat.geowave.core.ingest.spark;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameter;

public class SparkCommandLineOptions
{

	@Parameter(names = {
		"-n",
		"--name"
	}, description = "The spark application name")
	private String appName = "Spark Ingest";

	@Parameter(names = {
		"-ho",
		"--host"
	}, description = "The spark driver host")
	private String host = "localhost";

	@Parameter(names = {
		"-m",
		"--master"
	}, description = "The spark master designation")
	private String master = "local";

	@Parameter(names = {
		"-e",
		"--numexecutors"
	}, description = "Number of executors")
	private int numExecutors = -1;

	@Parameter(names = {
		"-c",
		"--numcores"
	}, description = "Number of cores")
	private int numCores = -1;

	public SparkCommandLineOptions() {}

	public String getAppName() {
		return appName;
	}

	public void setAppName(
			String appName ) {
		this.appName = appName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(
			String host ) {
		this.host = host;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(
			String master ) {
		this.master = master;
	}

	public int getNumExecutors() {
		return numExecutors;
	}

	public void setNumExecutors(
			int numExecutors ) {
		this.numExecutors = numExecutors;
	}

	public int getNumCores() {
		return numCores;
	}

	public void setNumCores(
			int numCores ) {
		this.numCores = numCores;
	}

}