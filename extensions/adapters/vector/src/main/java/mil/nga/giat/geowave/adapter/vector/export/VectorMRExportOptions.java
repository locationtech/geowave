package mil.nga.giat.geowave.adapter.vector.export;

import com.beust.jcommander.Parameter;

public class VectorMRExportOptions extends
		VectorExportOptions
{
	@Parameter(names = "--resourceManagerHostPort")
	private String resourceManagerHostPort;

	@Parameter(names = "--minSplits", description = "The min partitions for the input data")
	private Integer minSplits;

	@Parameter(names = "--maxSplits", description = "The max partitions for the input data")
	private Integer maxSplits;

	public Integer getMinSplits() {
		return minSplits;
	}

	public Integer getMaxSplits() {
		return maxSplits;
	}

	public String getResourceManagerHostPort() {
		return resourceManagerHostPort;
	}

	public void setResourceManagerHostPort(
			String resourceManagerHostPort ) {
		this.resourceManagerHostPort = resourceManagerHostPort;
	}

	public void setMinSplits(
			Integer minSplits ) {
		this.minSplits = minSplits;
	}

	public void setMaxSplits(
			Integer maxSplits ) {
		this.maxSplits = maxSplits;
	}
}
