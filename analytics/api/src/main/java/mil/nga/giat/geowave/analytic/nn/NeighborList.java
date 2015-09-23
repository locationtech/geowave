package mil.nga.giat.geowave.analytic.nn;

import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public interface NeighborList<NNTYPE> extends
		Iterable<Entry<ByteArrayId, NNTYPE>>
{
	public enum InferType {
		NONE,
		SKIP, // distance measure is skipped
		REMOVE // skipped and removed from future selection
	};

	/**
	 * May be called prior to init() when discovered by entry itself.
	 * 
	 * @param entry
	 * @return
	 */
	public boolean add(
			DistanceProfile<?> distanceProfile,
			ByteArrayId id,
			NNTYPE value );

	/**
	 * See if the entries relationships have already been inferred
	 * 
	 * @param entry
	 * @return
	 */
	public InferType infer(
			final ByteArrayId id,
			final NNTYPE value );

	/**
	 * Clear the contents.
	 */
	public void clear();

	public int size();

	public boolean isEmpty();

}
