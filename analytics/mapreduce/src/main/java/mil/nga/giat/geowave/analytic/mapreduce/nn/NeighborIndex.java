package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * Maintain an association between an ID of any item and its neighbors, as they
 * are discovered. The index supports a bi-directional association, forming a
 * graph of adjacency lists.
 * 
 * 
 * @param <NNTYPE>
 */
public class NeighborIndex<NNTYPE>
{
	private final Map<ByteArrayId, NeighborList<NNTYPE>> index = new HashMap<ByteArrayId, NeighborList<NNTYPE>>();
	private final NeighborListFactory<NNTYPE> listFactory;

	private final NullList<NNTYPE> nullList = new NullList<NNTYPE>();

	public NeighborIndex(
			final NeighborListFactory<NNTYPE> listFactory ) {
		super();
		this.listFactory = listFactory;
	}

	/**
	 * Invoked when the provided node is being inspected to find neighbors.
	 * Creates the associated neighbor list, if not already created. Notifies
	 * the neighbor list that it is formally initialized. The neighbor list may
	 * already exist and have associated neighbors. This occurs when those
	 * relationships are discovered through traversing the neighbor.
	 * 
	 * This method is designed for neighbor lists do some optimizations just
	 * prior to the neighbor discovery process.
	 * 
	 * @param node
	 * @return
	 */
	public NeighborList<NNTYPE> init(
			final Map.Entry<ByteArrayId, NNTYPE> node ) {
		NeighborList<NNTYPE> neighbors = index.get(node.getKey());
		if (neighbors == null) {
			neighbors = listFactory.buildNeighborList(
					node.getKey(),
					node.getValue());
			index.put(
					node.getKey(),
					neighbors);
		}
		neighbors.init();
		return neighbors;
	}

	public void add(
			final DistanceProfile<?> distanceProfile,
			final Map.Entry<ByteArrayId, NNTYPE> node,
			final Map.Entry<ByteArrayId, NNTYPE> neighbor,
			final boolean addReciprical ) {
		this.addToList(
				distanceProfile,
				node,
				neighbor);
		if (addReciprical) {
			this.addToList(
					distanceProfile,
					neighbor,
					node);
		}
	}

	public void empty(
			final ByteArrayId id ) {
		index.put(
				id,
				nullList);
	}

	private void addToList(
			final DistanceProfile<?> distanceProfile,
			final Map.Entry<ByteArrayId, NNTYPE> center,
			final Map.Entry<ByteArrayId, NNTYPE> neighbor ) {
		NeighborList<NNTYPE> neighbors = index.get(center.getKey());
		if (neighbors == null) {
			neighbors = listFactory.buildNeighborList(
					center.getKey(),
					center.getValue());
			index.put(
					center.getKey(),
					neighbors);
		}
		neighbors.add(
				distanceProfile,
				neighbor);
	}

}
