package org.locationtech.geowave.datastore.redis.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
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
	List<GeoWaveRedisPersistedRow> mergedRows;

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

	@Override
	public void mergeRow(
			final MergeableGeoWaveRow row ) {
		super.mergeRow(
				row);
		if (row instanceof GeoWaveRedisRow) {
			// this is intentionally not threadsafe because it isn't required
			if (mergedRows == null) {
				mergedRows = new ArrayList<>();
			}
			Arrays
					.stream(
							((GeoWaveRedisRow) row).getPersistedRows())
					.forEach(
							r -> mergedRows
									.add(
											r));
		}
	}

	public GeoWaveRedisPersistedRow[] getPersistedRows() {
		// this is intentionally not threadsafe because it isn't required
		if (mergedRows == null) {
			return new GeoWaveRedisPersistedRow[] {
				persistedRow
			};
		}
		else {
			return ArrayUtils.add(
					mergedRows.toArray(new GeoWaveRedisPersistedRow[0]),
					persistedRow);
		}
	}
}
