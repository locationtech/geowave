package mil.nga.giat.geowave.adapter.vector.export;

import com.beust.jcommander.Parameter;

public class VectorMRExportOptions extends
		VectorExportOptions
{
	@Parameter(names = "--resourceManagerHostPort")
	private String resourceManagerHostPort;

	@Parameter(names = "--minSplits")
	private int minSplits;

	@Parameter(names = "--maxSplits")
	private int maxSplits;

	public int getMinSplits() {
		return minSplits;
	}

	public int getMaxSplits() {
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
			int minSplits ) {
		this.minSplits = minSplits;
	}

	public void setMaxSplits(
			int maxSplits ) {
		this.maxSplits = maxSplits;
	}
}
