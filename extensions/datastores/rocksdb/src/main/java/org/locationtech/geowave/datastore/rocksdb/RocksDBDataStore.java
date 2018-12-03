package org.locationtech.geowave.datastore.rocksdb;

import java.io.Closeable;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import org.locationtech.geowave.datastore.rocksdb.operations.RocksDBOperations;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;

public class RocksDBDataStore extends
		BaseMapReduceDataStore implements
		Closeable
{
	public RocksDBDataStore(
			final RocksDBOperations operations,
			final DataStoreOptions options ) {
		super(
				new IndexStoreImpl(
						operations,
						options),
				new AdapterStoreImpl(
						operations,
						options),
				new DataStatisticsStoreImpl(
						operations,
						options),
				new AdapterIndexMappingStoreImpl(
						operations,
						options),
				new SecondaryIndexStoreImpl(),
				operations,
				options,
				new InternalAdapterStoreImpl(
						operations));
	}

	@Override
	public void close() {
		((RocksDBOperations) baseOperations).close();
	}
}
