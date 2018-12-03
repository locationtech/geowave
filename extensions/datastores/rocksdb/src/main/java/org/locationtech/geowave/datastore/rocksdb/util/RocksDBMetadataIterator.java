package org.locationtech.geowave.datastore.rocksdb.util;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public class RocksDBMetadataIterator extends
		AbstractRocksDBIterator<GeoWaveMetadata>
{
	private final boolean containsTimestamp;

	public RocksDBMetadataIterator(
			final RocksIterator it,
			final boolean containsTimestamp ) {
		this(
				null,
				it,
				containsTimestamp);
	}

	public RocksDBMetadataIterator(
			final ReadOptions options,
			final RocksIterator it,
			final boolean containsTimestamp ) {
		super(
				options,
				it);
		this.options = options;
		this.it = it;
		this.containsTimestamp = containsTimestamp;
	}

	@Override
	protected GeoWaveMetadata readRow(
			final byte[] key,
			final byte[] value ) {
		final ByteBuffer buf = ByteBuffer.wrap(key);
		final byte[] primaryId = new byte[Byte.toUnsignedInt(key[key.length - 2])];
		final byte[] visibility = new byte[Byte.toUnsignedInt(key[key.length - 1])];
		final byte[] secondaryId = new byte[containsTimestamp ? key.length - primaryId.length - visibility.length - 10
				: key.length - primaryId.length - visibility.length - 2];
		buf.get(primaryId);
		buf.get(secondaryId);
		if (containsTimestamp) {
			// just skip 8 bytes - we don't care to parse out the timestamp but
			// its there for key uniqueness and to maintain expected sort order
			buf.position(buf.position() + 8);
		}
		buf.get(visibility);

		return new RocksDBGeoWaveMetadata(
				primaryId,
				secondaryId,
				visibility,
				value,
				key);
	}

}
