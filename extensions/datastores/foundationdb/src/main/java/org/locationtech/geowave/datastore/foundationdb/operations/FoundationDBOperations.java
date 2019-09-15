package org.locationtech.geowave.datastore.foundationdb.operations;

import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.*;
import org.locationtech.geowave.datastore.foundationdb.config.FoundationDBOptions;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import java.io.Closeable;
import java.io.IOException;

// TODO: implement this
public class FoundationDBOperations implements MapReduceDataStoreOperations, Closeable {
  public FoundationDBOperations(FoundationDBOptions options) {}

  @Override
  public void close() throws IOException {

  }

  @Override
  public RowReader<GeoWaveRow> createReader(RecordReaderParams readerParams) {
    return null;
  }

  @Override
  public boolean indexExists(String indexName) throws IOException {
    return false;
  }

  @Override
  public boolean metadataExists(MetadataType type) throws IOException {
    return false;
  }

  @Override
  public void deleteAll() throws Exception {

  }

  @Override
  public boolean deleteAll(
      String indexName,
      String typeName,
      Short adapterId,
      String... additionalAuthorizations) {
    return false;
  }

  @Override
  public boolean ensureAuthorizations(String clientUser, String... authorizations) {
    return false;
  }

  @Override
  public RowWriter createWriter(Index index, InternalDataAdapter<?> adapter) {
    return null;
  }

  @Override
  public MetadataWriter createMetadataWriter(MetadataType metadataType) {
    return null;
  }

  @Override
  public MetadataReader createMetadataReader(MetadataType metadataType) {
    return null;
  }

  @Override
  public MetadataDeleter createMetadataDeleter(MetadataType metadataType) {
    return null;
  }

  @Override
  public <T> RowReader<T> createReader(ReaderParams<T> readerParams) {
    return null;
  }

  @Override
  public RowDeleter createRowDeleter(
      String indexName,
      PersistentAdapterStore adapterStore,
      InternalAdapterStore internalAdapterStore,
      String... authorizations) {
    return null;
  }
}
