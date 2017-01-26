package mil.nga.giat.geowave.core.index;

import java.util.List;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public interface IndexMetaData extends
		Persistable,
		Mergeable
{
	/**
	 * Update the aggregation result using the new entry provided
	 *
	 * @param insertionIds
	 *            the new indices to compute an updated aggregation result on
	 */
	public void insertionIdsAdded(
			List<ByteArrayId> insertionIds );

	/**
	 * Update the aggregation result by removing the entries provided
	 *
	 * @param insertionIds
	 *            the new indices to compute an updated aggregation result on
	 */
	public void insertionIdsRemoved(
			List<ByteArrayId> insertionIds );

	/**
	 * Create a JSON object that shows all the metadata handled by this object
	 * 
	 */
	public JSONObject toJSONObject()
			throws JSONException;

}
