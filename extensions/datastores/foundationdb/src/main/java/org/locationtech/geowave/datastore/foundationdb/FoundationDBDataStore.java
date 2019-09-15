package org.locationtech.geowave.datastore.foundationdb;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.metadata.*;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;

import java.io.Closeable;
import java.io.IOException;

public class FoundationDBDataStore extends BaseMapReduceDataStore implements Closeable {
    // TODO: implement FoundationDBOperations
    public FoundationDBDataStore(final MapReduceDataStoreOperations operations, final DataStoreOptions options) {
        super(
                new IndexStoreImpl(operations, options),
                new AdapterStoreImpl(operations, options),
                new DataStatisticsStoreImpl(operations, options),
                new AdapterIndexMappingStoreImpl(operations, options),
                operations,
                options,
                new InternalAdapterStoreImpl(operations));
    }

    @Override
    public void close() throws IOException {
        // TODO: implement FoundationDBOperations
    }
}
