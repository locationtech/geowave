package mil.nga.giat.geowave.core.store.filter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class AdapterIdQueryFilter implements
		DistributableQueryFilter
{
	private ByteArrayId adapterId;

	protected AdapterIdQueryFilter() {}

	public AdapterIdQueryFilter(
			final ByteArrayId adapterId ) {
		this.adapterId = adapterId;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		return (adapterId == null) || adapterId.equals(persistenceEncoding.getAdapterId());
	}

	@Override
	public byte[] toBinary() {
		if (adapterId == null) {
			return new byte[] {};
		}
		return adapterId.getBytes();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length == 0) {
			adapterId = null;
		}
		else {
			adapterId = new ByteArrayId(
					bytes);
		}
	}

}
