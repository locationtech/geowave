package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.File;

import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class RocksDBIndexTable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBIndexTable.class);
	private RocksDB writeDb;
	private RocksDB readDb;
	private long prevTime = Long.MAX_VALUE;
	private final Options writeOptions;
	private final Options readOptions;
	private final String subDirectory;
	private final boolean requiresTimestamp;
	private boolean readerDirty = false;
	private boolean exists;
	private final short adapterId;
	private final byte[] partition;

	public RocksDBIndexTable(
			final Options writeOptions,
			final Options readOptions,
			final String subDirectory,
			final short adapterId,
			final byte[] partition,
			final boolean requiresTimestamp ) {
		super();
		this.writeOptions = writeOptions;
		this.readOptions = readOptions;
		this.subDirectory = subDirectory;
		this.requiresTimestamp = requiresTimestamp;
		this.adapterId = adapterId;
		this.partition = partition;
		exists = new File(
				subDirectory).exists();
	}

	public synchronized void add(
			final byte[] sortKey,
			final byte[] dataId,
			final short numDuplicates,
			final GeoWaveValue value ) {
		byte[] key;
		if (requiresTimestamp) {
			// sometimes rows can be written so quickly that they are the exact
			// same millisecond - while Java does offer nanosecond precision,
			// support is OS-dependent. Instead this check is done to ensure
			// subsequent millis are written at least within this ingest
			// process.
			long time = Long.MAX_VALUE - System.currentTimeMillis();
			if (time >= prevTime) {
				time = prevTime - 1;
			}
			prevTime = time;
			key = Bytes.concat(
					sortKey,
					dataId,
					Longs.toByteArray(time),
					value.getFieldMask(),
					value.getVisibility(),
					ByteArrayUtils.shortToByteArray(numDuplicates),
					new byte[] {
						(byte) sortKey.length,
						(byte) value.getFieldMask().length,
						(byte) value.getVisibility().length
					});
		}
		else {
			key = Bytes.concat(
					sortKey,
					dataId,
					value.getFieldMask(),
					value.getVisibility(),
					ByteArrayUtils.shortToByteArray(numDuplicates),
					new byte[] {
						(byte) sortKey.length,
						(byte) value.getFieldMask().length,
						(byte) value.getVisibility().length,
					});
		}
		put(
				key,
				value.getValue());
	}

	public synchronized void delete(
			final byte[] key ) {
		final RocksDB db = getWriteDb();
		try {
			readerDirty = true;
			db.singleDelete(key);
		}
		catch (final RocksDBException e) {
			LOGGER.warn(
					"Unable to delete key",
					e);
		}
	}

	public synchronized CloseableIterator<GeoWaveRow> iterator() {
		final RocksDB readDb = getReadDb();
		if (readDb == null) {
			return new CloseableIterator.Empty<>();
		}
		final ReadOptions options = new ReadOptions().setFillCache(false);
		final RocksIterator it = readDb.newIterator(options);
		it.seekToFirst();
		return new RocksDBRowIterator(
				this,
				options,
				it,
				adapterId,
				partition,
				requiresTimestamp);
	}

	public synchronized CloseableIterator<GeoWaveRow> iterator(
			final ByteArrayRange range ) {
		final RocksDB readDb = getReadDb();
		if (readDb == null) {
			return new CloseableIterator.Empty<>();
		}
		final ReadOptions options;
		final RocksIterator it;
		if (range.getEnd() == null) {
			options = null;
			it = readDb.newIterator();
		}
		else {
			options = new ReadOptions().setIterateUpperBound(new Slice(
					range.getEndAsNextPrefix().getBytes()));
			it = readDb.newIterator(options);
		}
		if (range.getStart() == null) {
			it.seekToFirst();
		}
		else {
			it.seek(range.getStart().getBytes());
		}

		return new RocksDBRowIterator(
				this,
				options,
				it,
				adapterId,
				partition,
				requiresTimestamp);
	}

	private synchronized void put(
			final byte[] key,
			final byte[] value ) {
		// TODO batch writes
		final RocksDB db = getWriteDb();
		try {
			readerDirty = true;
			db.put(
					key,
					value);
		}
		catch (final RocksDBException e) {
			LOGGER.warn(
					"Unable to write key-value",
					e);
		}
	}

	@SuppressFBWarnings(justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
	public void flush() {
		// TODO flush batch writes
		final RocksDB db = getWriteDb();
		try {
			db.compactRange();
		}
		catch (final RocksDBException e) {
			LOGGER.warn(
					"Unable to compact range",
					e);
		}
		// force re-opening a reader to catch the updates from this write
		if (readerDirty && (readDb != null)) {
			synchronized (this) {
				if (readDb != null) {
					readDb.close();
					readDb = null;
				}
			}
		}
	}

	public void close() {
		synchronized (this) {
			if (writeDb != null) {
				writeDb.close();
				writeDb = null;
			}
			if (readDb != null) {
				readDb.close();
			}

		}
	}

	@SuppressFBWarnings(justification = "double check for null is intentional to avoid synchronized blocks when not needed.")
	private RocksDB getWriteDb() {
		// avoid synchronization if unnecessary by checking for null outside
		// synchronized block
		if (writeDb == null) {
			synchronized (this) {
				// check again within synchronized block
				if (writeDb == null) {
					try {
						if (exists || new File(
								subDirectory).mkdirs()) {
							exists = true;
							writeDb = RocksDB.open(
									writeOptions,
									subDirectory);
						}
						else {
							LOGGER.error("Unable to open to create directory '" + subDirectory + "'");
						}
					}
					catch (final RocksDBException e) {
						LOGGER.error(
								"Unable to open for writing",
								e);
					}
				}
			}
		}
		return writeDb;
	}

	@SuppressFBWarnings(justification = "double check for null is intentional to avoid synchronized blocks when not needed.")
	private RocksDB getReadDb() {
		if (!exists) {
			return null;
		}
		// avoid synchronization if unnecessary by checking for null outside
		// synchronized block
		if (readDb == null) {
			synchronized (this) {
				// check again within synchronized block
				if (readDb == null) {
					try {
						readerDirty = false;
						readDb = RocksDB.openReadOnly(
								readOptions,
								subDirectory);
					}
					catch (final RocksDBException e) {
						LOGGER.warn(
								"Unable to open for reading",
								e);
					}
				}
			}
		}
		return readDb;
	}
}
