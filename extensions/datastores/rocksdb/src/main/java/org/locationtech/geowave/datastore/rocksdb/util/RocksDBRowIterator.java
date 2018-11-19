package org.locationtech.geowave.datastore.rocksdb.util;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public class RocksDBRowIterator extends
		AbstractRocksDBIterator<GeoWaveRow>
{
	private final short adapterId;
	private final byte[] partition;
	private final boolean containsTimestamp;

	public RocksDBRowIterator(
			Object mutex,
			final ReadOptions options,
			final RocksIterator it,
			final short adapterId,
			final byte[] partition,
			final boolean containsTimestamp ) {
		super(
				options,
				it);
		this.adapterId = adapterId;
		this.partition = partition;
		this.containsTimestamp = containsTimestamp;
	}

	@Override
	protected GeoWaveRow readRow(
			final byte[] key,
			final byte[] value ) {
		return new RocksDBRow(
				adapterId,
				partition,
				key,
				value,
				containsTimestamp);
	}
}
