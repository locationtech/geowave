package mil.nga.giat.geowave.ingest.hdfs;

import java.io.File;

import mil.nga.giat.geowave.ingest.local.LocalPluginBase;

public interface StageToHdfsPlugin<T> extends
		HdfsPluginBase,
		LocalPluginBase
{
	public T[] toHdfsObjects(
			File f );
}
