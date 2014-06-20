package mil.nga.giat.geowave.ingest.local;

import java.io.File;

public interface LocalPluginBase
{
	public String[] getFileExtensionFilters();

	public void init(
			File baseDirectory );

	public boolean supportsFile(
			File file );
}
