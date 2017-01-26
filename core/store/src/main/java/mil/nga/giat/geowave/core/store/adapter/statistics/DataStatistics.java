package mil.nga.giat.geowave.core.store.adapter.statistics;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;

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

	public JSONObject toJSONObject()
			throws JSONException;
}
