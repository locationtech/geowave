package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.JumpParameters;
import mil.nga.giat.geowave.analytic.param.annotations.JumpParameter;

public class KMeansJumpOptions
{

	@JumpParameter(JumpParameters.Jump.COUNT_OF_CENTROIDS)
	@Parameter(names = {
		"-jcc",
		"--jumpCountOfCentroids"
	}, description = "Set the count of centroids for one run of kmeans.")
	private String jumpCountOfCentroids;

	@JumpParameter(JumpParameters.Jump.KPLUSPLUS_MIN)
	@Parameter(names = {
		"-jkp",
		"--jumpKplusplusMin"
	}, description = "The minimum k when K means ++ takes over sampling.")
	private String jumpKplusplusMin;

	@JumpParameter(JumpParameters.Jump.RANGE_OF_CENTROIDS)
	@Parameter(names = {
		"-jrc",
		"--jumpRangeOfCentroids"
	}, description = "Comma-separated range of centroids (e.g. 2,100)")
	private String jumpRangeOfCentroids;

	public String getJumpCountOfCentroids() {
		return jumpCountOfCentroids;
	}

	public void setJumpCountOfCentroids(
			String jumpCountOfCentroids ) {
		this.jumpCountOfCentroids = jumpCountOfCentroids;
	}

	public String getJumpKplusplusMin() {
		return jumpKplusplusMin;
	}

	public void setJumpKplusplusMin(
			String jumpKplusplusMin ) {
		this.jumpKplusplusMin = jumpKplusplusMin;
	}

	public String getJumpRangeOfCentroids() {
		return jumpRangeOfCentroids;
	}

	public void setJumpRangeOfCentroids(
			String jumpRangeOfCentroids ) {
		this.jumpRangeOfCentroids = jumpRangeOfCentroids;
	}
}
