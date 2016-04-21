package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.JumpParameters;
import mil.nga.giat.geowave.analytic.param.SampleParameters;
import mil.nga.giat.geowave.analytic.param.annotations.JumpParameter;
import mil.nga.giat.geowave.analytic.param.annotations.SampleParameter;

public class KMeansJumpOptions
{

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

	@SampleParameter(SampleParameters.Sample.SAMPLE_RANK_FUNCTION)
	@Parameter(names = {
		"-srf",
		"--sampleSampleRankFunction"
	}, description = "The rank function used when sampling the first N highest rank items.")
	private String sampleSampleRankFunction;

	@SampleParameter(SampleParameters.Sample.SAMPLE_SIZE)
	@Parameter(names = {
		"-sss",
		"--sampleSampleSize"
	}, description = "Sample Size")
	private String sampleSampleSize;

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

	public String getSampleSampleRankFunction() {
		return sampleSampleRankFunction;
	}

	public void setSampleSampleRankFunction(
			String sampleSampleRankFunction ) {
		this.sampleSampleRankFunction = sampleSampleRankFunction;
	}

	public String getSampleSampleSize() {
		return sampleSampleSize;
	}

	public void setSampleSampleSize(
			String sampleSampleSize ) {
		this.sampleSampleSize = sampleSampleSize;
	}
}
