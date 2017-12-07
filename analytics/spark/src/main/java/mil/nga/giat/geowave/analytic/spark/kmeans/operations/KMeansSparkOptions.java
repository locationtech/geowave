package mil.nga.giat.geowave.analytic.spark.kmeans.operations;

import com.beust.jcommander.Parameter;

public class KMeansSparkOptions
{
	@Parameter(names = {
		"-n",
		"--name"
	}, description = "The spark application name")
	private String appName = "KMeans Spark";

	@Parameter(names = {
		"-ho",
		"--host"
	}, description = "The spark driver host")
	private String host = "localhost";

	@Parameter(names = {
		"-m",
		"--master"
	}, description = "The spark master designation")
	private String master = "yarn";

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
		"-ch",
		"--computeHullData"
	}, description = "Compute hull count, area and density?")
	private Boolean computeHullData = false;

	@Parameter(names = "--cqlFilter", description = "An optional CQL filter applied to the input data")
	private String cqlFilter = null;

	@Parameter(names = {
		"-f",
		"--featureType"
	}, description = "Feature type name (adapter ID) to query")
	private String adapterId = null;

	@Parameter(names = "--minSplits", description = "The min partitions for the input data")
	private Integer minSplits = -1;

	@Parameter(names = "--maxSplits", description = "The max partitions for the input data")
	private Integer maxSplits = -1;

	@Parameter(names = {
		"-ct",
		"--centroidType"
	}, description = "Feature type name (adapter ID) for centroid output")
	private String centroidTypeName = "kmeans_centroids";

	@Parameter(names = {
		"-ht",
		"--hullType"
	}, description = "Feature type name (adapter ID) for hull output")
	private String hullTypeName = "kmeans_hulls";

	public KMeansSparkOptions() {}

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

	public Boolean isComputeHullData() {
		return computeHullData;
	}

	public void setComputeHullData(
			Boolean computeHullData ) {
		this.computeHullData = computeHullData;
	}

	public String getCqlFilter() {
		return cqlFilter;
	}

	public void setCqlFilter(
			String cqlFilter ) {
		this.cqlFilter = cqlFilter;
	}

	public String getAdapterId() {
		return adapterId;
	}

	public void setAdapterId(
			String adapterId ) {
		this.adapterId = adapterId;
	}

	public Integer getMinSplits() {
		return minSplits;
	}

	public void setMinSplits(
			Integer minSplits ) {
		this.minSplits = minSplits;
	}

	public Integer getMaxSplits() {
		return maxSplits;
	}

	public void setMaxSplits(
			Integer maxSplits ) {
		this.maxSplits = maxSplits;
	}

	public String getCentroidTypeName() {
		return centroidTypeName;
	}

	public void setCentroidTypeName(
			String centroidTypeName ) {
		this.centroidTypeName = centroidTypeName;
	}

	public String getHullTypeName() {
		return hullTypeName;
	}

	public void setHullTypeName(
			String hullTypeName ) {
		this.hullTypeName = hullTypeName;
	}
}