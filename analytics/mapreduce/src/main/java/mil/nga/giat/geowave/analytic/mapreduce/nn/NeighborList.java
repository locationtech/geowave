package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public interface NeighborList<NNTYPE> extends
		Iterable<Entry<ByteArrayId, NNTYPE>>
{
	/**
	 * May be called prior to init() when discovered by entry itself.
	 * 
	 * @param entry
	 * @return
	 */
	public boolean add(
			DistanceProfile<?> distanceProfile,
			Entry<ByteArrayId, NNTYPE> entry );

	public boolean contains(
			ByteArrayId key );

	/**
	 * Clear the contents.
	 */
	public void clear();

	public int size();

	public boolean isEmpty();

	/**
	 * Called when the driving code begins a search for neighbors of associated
	 * item.
	 */
	public void init();
}
