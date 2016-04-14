package mil.nga.giat.geowave.adapter.vector.export;

public class VectorMRExportOptions extends
		VectorExportOptions
{
	private String hdfsHostPort;
	private String resourceManagerHostPort;
	private int minSplits;
	private int maxSplits;
	private String hdfsOutputDirectory;

	public int getMinSplits() {
		return minSplits;
	}

	public int getMaxSplits() {
		return maxSplits;
	}

	public String getHdfsOutputDirectory() {
		return hdfsOutputDirectory;
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public String getResourceManagerHostPort() {
		return resourceManagerHostPort;
	}

	public void setHdfsHostPort(
			String hdfsHostPort ) {
		this.hdfsHostPort = hdfsHostPort;
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

	public void setHdfsOutputDirectory(
			String hdfsOutputDirectory ) {
		this.hdfsOutputDirectory = hdfsOutputDirectory;
	}
}
