package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;

public class FoundationDBMetadataTable implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBMetadataTable.class);
    private final Database db;
    private final List<FDBWrite> writes;

    public FoundationDBMetadataTable(Database db) {
        this.db = db;
        this.writes = new LinkedList<>();
    }

    public void add(final GeoWaveMetadata value) {
        // TODO
    }

    public void write(final byte[] key, final byte[] value) {
        writes.add(new FDBWrite(key, value));
    }

    public void flush() {
        db.run(txn -> {
            this.writes.forEach(write -> write.add(txn));
            return null;
        });
    }

    public void close() {
        db.close();
    }
}
