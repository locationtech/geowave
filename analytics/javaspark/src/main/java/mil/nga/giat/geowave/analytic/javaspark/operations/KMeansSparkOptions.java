package mil.nga.giat.geowave.analytic.javaspark.operations;

import com.beust.jcommander.Parameter;

public class KMeansSparkOptions
{
	@Parameter(names = {
		"-n",
		"--name"
	}, description = "The spark application name")
	private String appName = "KMeans Spark";

	@Parameter(names = {
		"-m",
		"--master"
	}, description = "The spark master designation")
	private String master = "local";

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
	private Double epsilon = null;

	@Parameter(names = {
		"-t",
		"--useTime"
	}, description = "Use time field from input data")
	private Boolean useTime = false;

	@Parameter(names = {
		"-h",
		"--hulls"
	}, description = "Generate convex hulls?")
	private Boolean generateHulls = false;

	@Parameter(names = {
		"-b",
		"--bbox"
	}, description = "Bounding box for spatial query (LL-Lat LL-Lon UR-Lat UR-Lon)")
	private String bbox = null;

	@Parameter(names = {
		"-a",
		"--adapterId"
	}, description = "Adapter ID to query")
	private String adapterId = null;

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

	public Boolean isUseTime() {
		return useTime;
	}

	public void setUseTime(
			Boolean useTime ) {
		this.useTime = useTime;
	}

	public Boolean isGenerateHulls() {
		return generateHulls;
	}

	public void setGenerateHulls(
			Boolean generateHulls ) {
		this.generateHulls = generateHulls;
	}

	public String getBoundingBox() {
		return bbox;
	}

	public void setBoundingBox(
			String bbox ) {
		this.bbox = bbox;
	}

	public String getAdapterId() {
		return adapterId;
	}

	public void setAdapterId(
			String adapterId ) {
		this.adapterId = adapterId;
	}
}
