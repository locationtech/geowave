package mil.nga.giat.geowave.format.landsat8;

import com.beust.jcommander.Parameter;

public class Landsat8DownloadCommandLineOptions
{
	@Parameter(names = "--overwrite", description = "An option to overwrite images that are ingested in the local workspace directory.  By default it will keep an existing image rather than downloading it again.")
	private boolean overwriteIfExists;

	public Landsat8DownloadCommandLineOptions() {}

	public boolean isOverwriteIfExists() {
		return overwriteIfExists;
	}
}
