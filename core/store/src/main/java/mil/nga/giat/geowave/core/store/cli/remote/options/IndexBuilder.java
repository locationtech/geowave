package mil.nga.giat.geowave.core.store.cli.remote.options;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface IndexBuilder
{
	public PrimaryIndex createIndex();
}
