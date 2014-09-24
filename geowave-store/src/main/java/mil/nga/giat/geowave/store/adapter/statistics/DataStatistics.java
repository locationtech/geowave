package mil.nga.giat.geowave.store.adapter.statistics;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.store.IngestCallback;

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
