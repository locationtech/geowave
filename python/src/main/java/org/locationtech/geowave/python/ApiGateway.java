package org.locationtech.geowave.python;

import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import py4j.GatewayServer;

public class ApiGateway {
    // Some starter code
    public DataStore getRocksDB(String namespace, String dir) {
        RocksDBOptions ops = new RocksDBOptions(namespace);

        ops.setDirectory(dir);

        return DataStoreFactory.createDataStore(ops);
    }

    public int getSize(DataStore d) {
        return d.getIndices().length;
    }

    public static void main(String[] args) {
        GatewayServer server = new GatewayServer(new ApiGateway());

        server.start();
    }
}
