package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.Closeable;
import java.io.File;
import java.util.Map.Entry;

import org.locationtech.geowave.core.store.operations.MetadataType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBClient implements
		Closeable
{

	private static class CacheKey
	{
		protected final String directory;
		protected final boolean requiresTimestamp;

		public CacheKey(
				final String directory,
				final boolean requiresTimestamp ) {
			this.directory = directory;
			this.requiresTimestamp = requiresTimestamp;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final CacheKey other = (CacheKey) obj;
			if (directory == null) {
				if (other.directory != null) {
					return false;
				}
			}
			else if (!directory
					.equals(
							other.directory)) {
				return false;
			}
			return true;
		}
	}

	private static class IndexCacheKey extends
			CacheKey
	{
		protected final short adapterId;
		protected final byte[] partition;

		public IndexCacheKey(
				final String directory,
				final short adapterId,
				final byte[] partition,
				final boolean requiresTimestamp ) {
			super(
					directory,
					requiresTimestamp);
			this.adapterId = adapterId;
			this.partition = partition;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = (prime * result) + adapterId;
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (!super.equals(
					obj)) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final IndexCacheKey other = (IndexCacheKey) obj;
			if (adapterId != other.adapterId) {
				return false;
			}
			return true;
		}

	}

	private final Cache<String, CacheKey> keyCache = Caffeine.newBuilder().build();
	private final LoadingCache<IndexCacheKey, RocksDBIndexTable> indexTableCache = Caffeine
			.newBuilder()
			.build(
					key -> {
						return new RocksDBIndexTable(
								indexWriteOptions,
								indexReadOptions,
								key.directory,
								key.adapterId,
								key.partition,
								key.requiresTimestamp);
					});
	private final LoadingCache<CacheKey, RocksDBMetadataTable> metadataTableCache = Caffeine
			.newBuilder()
			.build(
					key -> {
						new File(
								key.directory).mkdirs();
						return new RocksDBMetadataTable(
								RocksDB
										.open(
												metadataOptions,
												key.directory),
								key.requiresTimestamp);
					});
	private final String subDirectory;

	protected static Options indexWriteOptions = null;
	protected static Options indexReadOptions = null;
	protected static Options metadataOptions = null;

	public RocksDBClient(
			final String subDirectory ) {
		this.subDirectory = subDirectory;
	}

	public String getSubDirectory() {
		return subDirectory;
	}

	public synchronized RocksDBIndexTable getIndexTable(
			final String tableName,
			final short adapterId,
			final byte[] partition,
			final boolean requiresTimestamp ) {
		if (indexWriteOptions == null) {
			RocksDB.loadLibrary();
			final int cores = Runtime.getRuntime().availableProcessors();
			indexWriteOptions = new Options()
					.setCreateIfMissing(
							true)
					.prepareForBulkLoad()
					.setIncreaseParallelism(
							cores);
			indexReadOptions = new Options()
					.setIncreaseParallelism(
							cores);
		}
		final String directory = subDirectory + "/" + tableName;
		return indexTableCache
				.get(
						(IndexCacheKey) keyCache
								.get(
										directory,
										d -> new IndexCacheKey(
												d,
												adapterId,
												partition,
												requiresTimestamp)));
	}

	public synchronized RocksDBMetadataTable getMetadataTable(
			final MetadataType type ) {
		if (metadataOptions == null) {
			RocksDB.loadLibrary();
			metadataOptions = new Options()
					.setCreateIfMissing(
							true)
					.optimizeForSmallDb();
		}
		final String directory = subDirectory + "/" + type.name();
		return metadataTableCache
				.get(
						keyCache
								.get(
										directory,
										d -> new CacheKey(
												d,
												type
														.equals(
																MetadataType.STATS))));
	}

	public boolean indexTableExists(
			final String indexName ) {
		// then look for prefixes of this index directory in which case there is
		// a partition key
		for (final String key : keyCache.asMap().keySet()) {
			if (key
					.substring(
							subDirectory.length())
					.contains(
							indexName)) {
				return true;
			}
		}
		// this could have been created by a different process so check the
		// directory listing
		String[] listing = new File(
				subDirectory)
						.list(
								(
										dir,
										name ) -> name
												.contains(
														indexName));
		return listing != null && listing.length > 0;
	}

	public boolean metadataTableExists(
			final MetadataType type ) {
		// this could have been created by a different process so check the
		// directory listing
		return (keyCache
				.getIfPresent(
						subDirectory + "/" + type.name()) != null)
				|| new File(
						subDirectory + "/" + type.name()).exists();
	}

	public void close(
			final String indexName,
			final String typeName ) {
		final String prefix = RocksDBUtils
				.getTablePrefix(
						typeName,
						indexName);
		for (final Entry<String, CacheKey> e : keyCache.asMap().entrySet()) {
			final String key = e.getKey();
			if (key
					.substring(
							subDirectory.length())
					.startsWith(
							prefix)) {
				keyCache
						.invalidate(
								key);
				final RocksDBIndexTable indexTable = indexTableCache
						.getIfPresent(
								e.getValue());
				if (indexTable != null) {
					indexTableCache
							.invalidate(
									e.getValue());
					indexTable.close();
				}

			}
		}
	}

	@Override
	public void close() {
		keyCache.invalidateAll();
		indexTableCache
				.asMap()
				.values()
				.forEach(
						db -> db.close());
		indexTableCache.invalidateAll();
		metadataTableCache
				.asMap()
				.values()
				.forEach(
						db -> db.close());
		metadataTableCache.invalidateAll();
	}
}
