package mil.nga.giat.geowave.analytic.javaspark.operations;

import com.beust.jcommander.Parameter;

public class KMeansSparkOptions
{
	@Parameter(names = {
		"-n",
		"--name"
	}, description = "The spark application name")
	private String appName;

	@Parameter(names = {
		"-m",
		"--master"
	}, description = "The spark master designation (default: local)")
	private String master;

	@Parameter(names = {
		"-k",
		"--numClusters"
	}, description = "The number of clusters to generate")
	private Integer numClusters = 8;

	@Parameter(names = {
		"-i",
		"--numIterations"
	}, description = "The number of iterations to run")
	private Integer numIterations = 20;

	@Parameter(names = {
		"-e",
		"--epsilon"
	}, description = "The convergence tolerance")
	private Double epsilon;

	public KMeansSparkOptions() {}

	public String getAppName() {
		return appName;
	}

	public void setAppName(
			String appName ) {
		this.appName = appName;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(
			String master ) {
		this.master = master;
	}

	public Integer getNumClusters() {
		return numClusters;
	}

	public void setNumClusters(
			Integer numClusters ) {
		this.numClusters = numClusters;
	}

	public Integer getNumIterations() {
		return numIterations;
	}

	public void setNumIterations(
			Integer numIterations ) {
		this.numIterations = numIterations;
	}

	public Double getEpsilon() {
		return epsilon;
	}

	public void setEpsilon(
			Double epsilon ) {
		this.epsilon = epsilon;
	}

}
