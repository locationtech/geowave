package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.IngestCallback;

public interface DataStatistics<T> extends
		Mergeable,
		IngestCallback<T>
{
	public ByteArrayId getDataAdapterId();

	public void setDataAdapterId(
			ByteArrayId dataAdapterId );

	public ByteArrayId getStatisticsId();

	public void setVisibility(
			byte[] visibility );

	public byte[] getVisibility();
}
