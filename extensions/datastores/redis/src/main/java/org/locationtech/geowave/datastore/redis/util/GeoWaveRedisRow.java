package org.locationtech.geowave.datastore.redis.util;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.MergeableGeoWaveRow;

public class GeoWaveRedisRow extends
		MergeableGeoWaveRow implements
		GeoWaveRow
{
	private final short adapterId;
	private final byte[] partitionKey;
	private final byte[] sortKey;
	private final GeoWaveRedisPersistedRow persistedRow;

	public GeoWaveRedisRow(
			final GeoWaveRedisPersistedRow persistedRow,
			final short adapterId,
			final byte[] partitionKey,
			final byte[] sortKey ) {
		super(
				new GeoWaveValue[] {
					persistedRow.getGeoWaveValue()
				});
		this.persistedRow = persistedRow;
		this.adapterId = adapterId;
		this.partitionKey = partitionKey;
		this.sortKey = sortKey;
	}

	@Override
	public byte[] getDataId() {
		return persistedRow.getDataId();
	}

	@Override
	public short getAdapterId() {
		return adapterId;
	}

	@Override
	public byte[] getSortKey() {
		return sortKey;
	}

	@Override
	public byte[] getPartitionKey() {
		return partitionKey;
	}

	@Override
	public int getNumberOfDuplicates() {
		return persistedRow.getNumDuplicates();
	}

	public GeoWaveRedisPersistedRow getPersistedRow() {
		return persistedRow;
	}

}
