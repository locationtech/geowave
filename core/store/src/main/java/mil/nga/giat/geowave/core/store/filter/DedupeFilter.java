package mil.nga.giat.geowave.core.store.filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;

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
		if (!persistenceEncoding.isDeduplicationEnabled()) {
			// certain types of data such as raster do not intend to be
			// duplicated
			// short circuit this check if the row is does not support
			// deduplication
			return true;
		}
		if (!supportsMultipleIndices() && !persistenceEncoding.isDuplicated()) {
			// short circuit this check if the row is not duplicated anywhere
			// and this is only intended to support a single index
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

	protected boolean supportsMultipleIndices() {
		return false;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

}
