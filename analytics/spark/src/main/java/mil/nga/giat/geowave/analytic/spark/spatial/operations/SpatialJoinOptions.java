package mil.nga.giat.geowave.analytic.spark.spatial.operations;

import com.beust.jcommander.Parameter;

public class SpatialJoinOptions
{
	@Parameter(names = {
		"-n",
		"--name"
	}, description = "The spark application name")
	private String appName = "Spatial Join Spark";

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
		"-pc",
		"--partCount",
	}, description = "The default partition count to set for Spark RDDs. Should be big enough to support largest RDD that will be used. Sets spark.default.parallelism")
	private Integer partCount = -1;

	@Parameter(names = {
		"-la",
		"--leftAdapter"
	}, description = "Feature type name (adapter ID) of left Store to use in join")
	private String leftAdapterId = null;

	@Parameter(names = {
		"-ol",
		"--outLeftAdapter"
	}, description = "Feature type name (adapter ID) of left join results.")
	private String outLeftAdapterId = null;

	@Parameter(names = {
		"-ra",
		"--rightAdapter"
	}, description = "Feature type name (adapter ID) of right Store to use in join")
	private String rightAdapterId = null;

	@Parameter(names = {
		"-or",
		"--outRightAdapter"
	}, description = "Feature type name (adapter ID) of right join results.")
	private String outRightAdapterId = null;

	@Parameter(names = {
		"-p",
		"--predicate"
	}, description = "Name of the UDF function to use when performing Spatial Join")
	private String predicate = "GeomIntersects";

	@Parameter(names = {
		"-r",
		"--radius",
	}, description = "Used for distance join predicate and other spatial operations that require a scalar radius.")
	private Double radius = 0.01;

	// TODO: Experiment with collecting + broadcasting rdds when one side can
	// fit into memory
	private boolean leftBroadcast = false;
	private boolean rightBroadcast = false;

	public SpatialJoinOptions() {}

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

	public Integer getPartCount() {
		return partCount;
	}

	public void setPartCount(
			Integer partCount ) {
		this.partCount = partCount;
	}

	public String getLeftAdapterId() {
		return leftAdapterId;
	}

	public void setLeftAdapterId(
			String leftAdapterId ) {
		this.leftAdapterId = leftAdapterId;
	}

	public String getRightAdapterId() {
		return rightAdapterId;
	}

	public void setRightAdapterId(
			String rightAdapterId ) {
		this.rightAdapterId = rightAdapterId;
	}

	public String getPredicate() {
		return predicate;
	}

	public void setPredicate(
			String predicate ) {
		this.predicate = predicate;
	}

	public Double getRadius() {
		return radius;
	}

	public void setRadius(
			Double radius ) {
		this.radius = radius;
	}

	public String getOutputLeftAdapterId() {
		return outLeftAdapterId;
	}

	public void setOutputLeftAdapterId(
			String outLeftAdapterId ) {
		this.outLeftAdapterId = outLeftAdapterId;
	}

	public String getOutputRightAdapterId() {
		return outRightAdapterId;
	}

	public void setOutputRightAdapterId(
			String outRightAdapterId ) {
		this.outRightAdapterId = outRightAdapterId;
	}
}
