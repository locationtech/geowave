package mil.nga.giat.geowave.adapter.raster.resize.options;

import com.beust.jcommander.Parameter;

public class RasterTileResizeCommandLineOptions
{
	@Parameter(names = "--inputCoverageName", description = "The name of the feature type to run a KDE on", required = true)
	private String inputCoverageName;

	@Parameter(names = "--outputCoverageName", description = "The min level to run a KDE at", required = true)
	private String outputCoverageName;

	@Parameter(names = "--minSplits", description = "The min partitions for the input data")
	private Integer minSplits;

	@Parameter(names = "--maxSplits", description = "The max partitions for the input data")
	private Integer maxSplits;

	@Parameter(names = "--hdfsHostPort", description = "The max partitions for the input data", converter = HdfsHostPortConverter.class)
	private String hdfsHostPort;

	@Parameter(names = "--jobSubmissionHostPort", description = "The max partitions for the input data")
	private String jobTrackerOrResourceManHostPort;

	@Parameter(names = "--outputTileSize", description = "The max partitions for the input data")
	private Integer outputTileSize;

	@Parameter(names = "--indexId", description = "The max level to run a KDE at")
	private String indexId;

	// Default constructor
	public RasterTileResizeCommandLineOptions() {

	}

	public RasterTileResizeCommandLineOptions(
			final String inputCoverageName,
			final String outputCoverageName,
			final Integer minSplits,
			final Integer maxSplits,
			final String hdfsHostPort,
			final String jobTrackerOrResourceManHostPort,
			final Integer outputTileSize,
			final String indexId ) {
		this.inputCoverageName = inputCoverageName;
		this.outputCoverageName = outputCoverageName;
		this.minSplits = minSplits;
		this.maxSplits = maxSplits;
		this.hdfsHostPort = hdfsHostPort;
		this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
		this.outputTileSize = outputTileSize;
		this.indexId = indexId;
	}

	public String getInputCoverageName() {
		return inputCoverageName;
	}

	public String getOutputCoverageName() {
		return outputCoverageName;
	}

	public Integer getMinSplits() {
		return minSplits;
	}

	public Integer getMaxSplits() {
		return maxSplits;
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public String getJobTrackerOrResourceManHostPort() {
		return jobTrackerOrResourceManHostPort;
	}

	public Integer getOutputTileSize() {
		return outputTileSize;
	}

	public String getIndexId() {
		return indexId;
	}

	public void setInputCoverageName(
			String inputCoverageName ) {
		this.inputCoverageName = inputCoverageName;
	}

	public void setOutputCoverageName(
			String outputCoverageName ) {
		this.outputCoverageName = outputCoverageName;
	}

	public void setMinSplits(
			Integer minSplits ) {
		this.minSplits = minSplits;
	}

	public void setMaxSplits(
			Integer maxSplits ) {
		this.maxSplits = maxSplits;
	}

	public void setHdfsHostPort(
			String hdfsHostPort ) {
		this.hdfsHostPort = hdfsHostPort;
	}

	public void setJobTrackerOrResourceManHostPort(
			String jobTrackerOrResourceManHostPort ) {
		this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
	}

	public void setOutputTileSize(
			Integer outputTileSize ) {
		this.outputTileSize = outputTileSize;
	}

	public void setIndexId(
			String indexId ) {
		this.indexId = indexId;
	}

}
