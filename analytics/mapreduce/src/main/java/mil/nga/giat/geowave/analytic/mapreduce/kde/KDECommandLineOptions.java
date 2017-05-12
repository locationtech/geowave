package mil.nga.giat.geowave.analytic.mapreduce.kde;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.mapreduce.operations.HdfsHostPortConverter;

public class KDECommandLineOptions
{
	@Parameter(names = "--featureType", required = true, description = "The name of the feature type to run a KDE on")
	private String featureType;

	@Parameter(names = "--indexId", description = "An optional index ID to filter the input data")
	private String indexId;

	@Parameter(names = "--minLevel", required = true, description = "The min level to run a KDE at")
	private Integer minLevel;

	@Parameter(names = "--maxLevel", required = true, description = "The max level to run a KDE at")
	private Integer maxLevel;

	@Parameter(names = "--minSplits", description = "The min partitions for the input data")
	private Integer minSplits;

	@Parameter(names = "--maxSplits", description = "The max partitions for the input data")
	private Integer maxSplits;

	@Parameter(names = "--coverageName", required = true, description = "The coverage name")
	private String coverageName;

	@Parameter(names = "--hdfsHostPort", required = true, description = "The hdfs host port", converter = HdfsHostPortConverter.class)
	private String hdfsHostPort;

	@Parameter(names = "--jobSubmissionHostPort", required = true, description = "The job submission tracker")
	private String jobTrackerOrResourceManHostPort;

	@Parameter(names = "--tileSize", description = "The tile size")
	private Integer tileSize = 1;

	@Parameter(names = "--cqlFilter", description = "An optional CQL filter applied to the input data")
	private String cqlFilter;

	public KDECommandLineOptions() {}

	public String getIndexId() {
		return indexId;
	}

	public String getFeatureType() {
		return featureType;
	}

	public Integer getMinLevel() {
		return minLevel;
	}

	public Integer getMaxLevel() {
		return maxLevel;
	}

	public Integer getMinSplits() {
		return minSplits;
	}

	public Integer getMaxSplits() {
		return maxSplits;
	}

	public String getCoverageName() {
		return coverageName;
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public String getJobTrackerOrResourceManHostPort() {
		return jobTrackerOrResourceManHostPort;
	}

	public Integer getTileSize() {
		return tileSize;
	}

	public String getCqlFilter() {
		return cqlFilter;
	}

	public void setFeatureType(
			String featureType ) {
		this.featureType = featureType;
	}

	public void setIndexId(
			String indexId ) {
		this.indexId = indexId;
	}

	public void setMinLevel(
			Integer minLevel ) {
		this.minLevel = minLevel;
	}

	public void setMaxLevel(
			Integer maxLevel ) {
		this.maxLevel = maxLevel;
	}

	public void setMinSplits(
			Integer minSplits ) {
		this.minSplits = minSplits;
	}

	public void setMaxSplits(
			Integer maxSplits ) {
		this.maxSplits = maxSplits;
	}

	public void setCoverageName(
			String coverageName ) {
		this.coverageName = coverageName;
	}

	public void setHdfsHostPort(
			String hdfsHostPort ) {
		this.hdfsHostPort = hdfsHostPort;
	}

	public void setJobTrackerOrResourceManHostPort(
			String jobTrackerOrResourceManHostPort ) {
		this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
	}

	public void setTileSize(
			Integer tileSize ) {
		this.tileSize = tileSize;
	}

	public void setCqlFilter(
			String cqlFilter ) {
		this.cqlFilter = cqlFilter;
	}
}