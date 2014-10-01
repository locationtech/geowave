package mil.nga.giat.geowave.store.filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.IndexedPersistenceEncoding;

/**
 * This filter will perform de-duplication using the combination of data adapter
 * ID and data ID to determine uniqueness. It can be performed client-side
 * and/or distributed.
 * 
 */
public class DedupeFilter implements
		DistributableQueryFilter
{
	private final Map<ByteArrayId, Set<ByteArrayId>> adapterIdToVisitedDataIdMap;

	public DedupeFilter() {
		adapterIdToVisitedDataIdMap = new HashMap<ByteArrayId, Set<ByteArrayId>>();
	}

	@Override
	public boolean accept(
			final IndexedPersistenceEncoding persistenceEncoding ) {
		if (!persistenceEncoding.isDuplicated()) {
			// short circuit this check if the row is not duplicated anywhere
			return true;
		}
		final ByteArrayId adapterId = persistenceEncoding.getAdapterId();
		final ByteArrayId dataId = persistenceEncoding.getDataId();
		Set<ByteArrayId> visitedDataIds = adapterIdToVisitedDataIdMap.get(adapterId);
		if (visitedDataIds == null) {
			visitedDataIds = new HashSet<ByteArrayId>();
			adapterIdToVisitedDataIdMap.put(
					adapterId,
					visitedDataIds);
		}
		else if (visitedDataIds.contains(dataId)) {
			return false;
		}
		visitedDataIds.add(dataId);
		return true;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

}
